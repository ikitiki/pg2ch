package replicator

import (
	"context"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgtype"
)

func initPostgresql(ctx context.Context, conn *pgx.Conn) (error) {
	const (
		namedOIDQuery = `select t.oid,
	case when nsp.nspname in ('pg_catalog', 'public') then t.typname
		else nsp.nspname||'.'||t.typname
	end
from pg_type t
left join pg_type base_type on t.typelem=base_type.oid
left join pg_namespace nsp on t.typnamespace=nsp.oid
where (
	  t.typtype in('b', 'p', 'r', 'e')
	  and (base_type.oid is null or base_type.typtype in('b', 'p', 'r'))
	)`
	)

	rows, err := conn.Query(ctx, namedOIDQuery)
	if err != nil {
		return err
	}

	nameOIDs, err := connInfoFromRows(&rows)
	if err != nil {
		return err
	}

	cinfo := pgtype.NewConnInfo()
	conn.ConnInfo().InitializeDataTypes(nameOIDs)

	if err = initConnInfoEnumArray(ctx, conn, cinfo); err != nil {
		return err
	}

	return nil
}

func initConnInfoEnumArray(ctx context.Context, conn *pgx.Conn, cinfo *pgtype.ConnInfo) error {
	nameOIDs := make(map[string]uint32, 16)
	rows, err := conn.Query(ctx, `select t.oid, t.typname
from pg_type t
  join pg_type base_type on t.typelem=base_type.oid
where t.typtype = 'b'
  and base_type.typtype = 'e'`)
	if err != nil {
		return err
	}

	for rows.Next() {
		var oid uint32
		var name pgtype.Text
		if err := rows.Scan(&oid, &name); err != nil {
			return err
		}

		nameOIDs[name.String] = oid
	}

	if rows.Err() != nil {
		return rows.Err()
	}

	for name, oid := range nameOIDs {
		cinfo.RegisterDataType(pgtype.DataType{
			Value: &pgtype.EnumArray{},
			Name:  name,
			OID:   oid,
		})
	}

	return nil
}

func connInfoFromRows(rows *pgx.Rows) (map[string]uint32, error) {
	defer (*rows).Close()

	nameOIDs := make(map[string]uint32, 256)
	for (*rows).Next() {
		var oid uint32
		var name pgtype.Text
		if err := (*rows).Scan(&oid, &name); err != nil {
			return nil, err
		}

		nameOIDs[name.String] = oid
	}

	if err := (*rows).Err(); err != nil {
		return nil, err
	}

	return nameOIDs, nil
}
