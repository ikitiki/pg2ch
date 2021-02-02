package consumer

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgproto3/v2"

	"github.com/mkabilov/pg2ch/pkg/decoder"
	"github.com/mkabilov/pg2ch/pkg/message"
	"github.com/mkabilov/pg2ch/pkg/utils"
)

const (
	statusTimeout   = time.Second * 10
	replWaitTimeout = time.Second * 10
)

// Handler represents interface for processing logical replication messages
type Handler interface {
	HandleMessage(utils.LSN, message.Message) error
}

// Interface represents interface for the consumer
type Interface interface {
	SendStatus() error
	Run(Handler) error
	AdvanceLSN(utils.LSN)
	Wait()
}

type consumer struct {
	waitGr          *sync.WaitGroup
	ctx             context.Context
	conn            *pgconn.PgConn
	dbCfg           pgconn.Config
	slotName        string
	publicationName string
	currentLSN      utils.LSN
	errCh           chan error
}

// New instantiates the consumer
func New(ctx context.Context, errCh chan error, dbCfg pgconn.Config, slotName, publicationName string, startLSN utils.LSN) *consumer {
	return &consumer{
		waitGr:          &sync.WaitGroup{},
		ctx:             ctx,
		dbCfg:           dbCfg,
		slotName:        slotName,
		publicationName: publicationName,
		currentLSN:      startLSN,
		errCh:           errCh,
	}
}

// AdvanceLSN advances lsn position
func (c *consumer) AdvanceLSN(lsn utils.LSN) {
	c.currentLSN = lsn
}

// Wait waits for the goroutines
func (c *consumer) Wait() {
	c.waitGr.Wait()
}

func (c *consumer) close(err error) {
	select {
	case c.errCh <- err:
	default:
	}
}

// Run runs consumer
func (c *consumer) Run(handler Handler) error {
	rc, err := pgconn.ConnectConfig(c.ctx, &c.dbCfg)
	if err != nil {
		return fmt.Errorf("could not connect using replication protocol: %v", err)
	}

	c.conn = rc

	if err := c.startDecoding(); err != nil {
		return fmt.Errorf("could not start replication slot: %v", err)
	}

	// we may have flushed the final segment at shutdown without bothering to advance the slot LSN.
	if err := c.SendStatus(); err != nil {
		return fmt.Errorf("could not send replay progress: %v", err)
	}

	c.waitGr.Add(1)
	go c.processReplicationMessage(handler)

	return nil
}

func (c *consumer) startDecoding() error {
	log.Printf("Starting from %s lsn", c.currentLSN)

	startReplOptions := pglogrepl.StartReplicationOptions{
		Timeline: -1,
		Mode: pglogrepl.LogicalReplication,
		PluginArgs: []string{`"proto_version" '1'`, fmt.Sprintf(`"publication_names" '%s'`, c.publicationName)},
	}

	err := pglogrepl.StartReplication(c.ctx, c.conn, c.slotName, pglogrepl.LSN(uint64(c.currentLSN)), startReplOptions)

	if err != nil {
		c.closeDbConnection()
		return fmt.Errorf("failed to start decoding logical replication messages: %v", err)
	}

	return nil
}

func (c *consumer) closeDbConnection() {
	if err := c.conn.Close(c.ctx); err != nil {
		log.Printf("could not close replication connection: %v", err)
	}
}

func (c *consumer) processReplicationMessage(handler Handler) {
	defer c.waitGr.Done()

	statusTicker := time.NewTicker(statusTimeout)

	for {
		select {
		case <-c.ctx.Done():
			statusTicker.Stop()
			c.closeDbConnection()
			return
		case <-statusTicker.C:
			if err := c.SendStatus(); err != nil {
				c.close(fmt.Errorf("could not send replay progress: %v", err))
				return
			}
		default:
			wctx, cancel := context.WithTimeout(c.ctx, replWaitTimeout)
			repMsg, err := c.conn.ReceiveMessage(wctx)
			cancel()

			if err == context.DeadlineExceeded {
				continue
			} else if err == context.Canceled {
				log.Printf("received shutdown request: decoding terminated")
				return
			} else if err != nil {
				// TODO: make sure we retry and cleanup after ourselves afterwards
				c.close(fmt.Errorf("replication failed: %v", err))
				return
			}

			if repMsg == nil {
				log.Printf("received null replication message")
				continue
			}

			//NEW
			switch repMsg := repMsg.(type) {
				case *pgproto3.CopyData:
					switch repMsg.Data[0] {
					case pglogrepl.PrimaryKeepaliveMessageByteID:
						pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(repMsg.Data[1:])
						if err != nil {
							log.Fatalln("ParsePrimaryKeepaliveMessage failed:", err)
						}
						log.Println("Primary Keepalive Message =>", "ServerWALEnd:", pkm.ServerWALEnd, "ServerTime:", pkm.ServerTime, "ReplyRequested:", pkm.ReplyRequested)

						//if pkm.ReplyRequested {
						//	nextStandbyMessageDeadline = time.Time{}
						//}

					case pglogrepl.XLogDataByteID:
						xld, err := pglogrepl.ParseXLogData(repMsg.Data[1:])
						if err != nil {
							log.Fatalln("ParseXLogData failed:", err)
						}
						log.Println("XLogData =>", "WALStart", xld.WALStart, "ServerWALEnd", xld.ServerWALEnd, "ServerTime:", xld.ServerTime, "WALData size", len(xld.WALData))

						//MAYBE clientXLogPos = xld.WALStart + pglogrepl.LSN(len(xld.WALData))
						msg, err := decoder.Parse(xld.WALData)
						if err != nil {
							c.close(fmt.Errorf("invalid pgoutput message: %s", err))
							return
						}

						if err := handler.HandleMessage(utils.LSN(xld.WALStart), msg); err != nil {
							c.close(fmt.Errorf("error handling waldata: %s", err))
							return
						}

					}
				default:
					log.Printf("Received unexpected message: %#v\n", repMsg)
				}
			// END NEW

			//if repMsg.ServerHeartbeat != nil && repMsg.ServerHeartbeat.ReplyRequested == 1 {
			//	log.Println("server wants a reply")
			//	if err := c.SendStatus(); err != nil {
			//		c.close(fmt.Errorf("could not send replay progress: %v", err))
			//		return
			//	}
			//}
		}
	}
}

// SendStatus sends the status
func (c *consumer) SendStatus() error {
	// log.Printf("sending status: %v", c.currentLSN) //TODO: move to debug log level
	status := pglogrepl.StandbyStatusUpdate{WALWritePosition: pglogrepl.LSN(uint64(c.currentLSN))}

	if err := pglogrepl.SendStandbyStatusUpdate(c.ctx, c.conn, status); err != nil {
		return fmt.Errorf("failed to send standy status: %s", err)
	}

	return nil
}
