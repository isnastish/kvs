package kvs

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/isnastish/kvs/pkg/log"
)

// NOTE: If the value has been deleted, hence "delete" event was sent.
// We have to clean up all the transactions, otherwise our transaction storage will
// keep growing and we eventually run out of memory or ids to enumerate the transactions.

type PostgresTxnLogger struct {
	eventsChan chan Event
	errorsChan chan error
	dbpool     *pgxpool.Pool
	wg         sync.WaitGroup

	// Once we establish clear table structure (schema), we can remove them.
	intKeyTable      string
	intTxnTable      string
	floatKeyTable    string
	floatTxnTable    string
	strKeyTable      string
	strTxnTable      string
	mapHashTable     string
	mapTxnTable      string
	mapKeyValueTable string
}

func NewDBTxnLogger(databaseURL string) (*PostgresTxnLogger, error) {
	dbconfig, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("failed to build a config %w", err)
	}

	dbconfig.BeforeAcquire = func(ctx context.Context, conn *pgx.Conn) bool {
		log.Logger.Info("Before acquiring connection from the pool")
		return true
	}

	dbconfig.AfterRelease = func(conn *pgx.Conn) bool {
		log.Logger.Info("After releasing connection from the pool")
		return true
	}

	dbconfig.BeforeClose = func(conn *pgx.Conn) {
		log.Logger.Info("Closing the connection")
	}

	dbpool, err := pgxpool.NewWithConfig(context.Background(), dbconfig)
	if err != nil {
		return nil, err
	}

	if err := dbpool.Ping(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to establish db connection %w", err)
	}

	logger := &PostgresTxnLogger{
		dbpool:     dbpool,
		wg:         sync.WaitGroup{},
		eventsChan: make(chan Event, 32),
		errorsChan: make(chan error, 1),

		// keep a list of table names in case we need to change them,
		// we can do it in one place, and it's easy to drop all the table at once
		intKeyTable:      "int_keys",
		intTxnTable:      "int_transactions",
		floatKeyTable:    "float_keys",
		floatTxnTable:    "float_transactions",
		strKeyTable:      "str_keys",
		strTxnTable:      "str_transactions",
		mapHashTable:     "map_hashes",
		mapTxnTable:      "map_transactions",
		mapKeyValueTable: "map_key_values",
	}

	// if err := logger.createTablesIfDontExist(); err != nil {
	// 	return nil, err
	// }

	return logger, nil
}

func (l *PostgresTxnLogger) WaitForPendingTransactions() {
	l.wg.Wait()
}

func (l *PostgresTxnLogger) dropTable(dbconn *pgxpool.Conn, table string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if _, err := dbconn.Exec(ctx, fmt.Sprintf(`drop table if exists "%s";`, table)); err != nil {
		return fmt.Errorf("failed to drop the table %s, %w", table, err)
	}

	return nil
}

func (l *PostgresTxnLogger) createTablesIfDontExist() error {
	timeout := 15 * time.Second

	dbconn, err := l.dbpool.Acquire(context.Background())
	if err != nil {
		return fmt.Errorf("failed to acquire db connection %w", err)
	}
	defer dbconn.Release()

	createTxnKeyTable := func(dbconn *pgxpool.Conn, tableName string) error {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		query := fmt.Sprintf(`create table if not exists "%s" (
			"id" serial primary key,
			"key" text not null unique);`, tableName)

		if _, err := dbconn.Exec(ctx, query); err != nil {
			return fmt.Errorf("failed to create %s table %w", tableName, err)
		}

		return nil
	}

	// TODO: Once we establish the structure of hwo the tables should look like,
	// we can factor them out into functions to avoid code duplications.
	// Maybe even create all the tables in a single batch?

	// integer transactions
	{
		if err := createTxnKeyTable(dbconn, l.intKeyTable); err != nil {
			return err
		}

		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		query := fmt.Sprintf(`create table if not exists "%s" (
			"id" serial primary key,
			"type" varchar(32) not null check("type" in ('put', 'get', 'delete', 'incr', 'incrby')),
			"key_id" integer,
			"value" integer,
			"timestamp" timestamp not null default now(),
			foreign key("key_id") references "%s"("id") on delete cascade);`, l.intTxnTable, l.intKeyTable)

		if _, err := dbconn.Exec(ctx, query); err != nil {
			return fmt.Errorf("failed to create %s table %w", l.intTxnTable, err)
		}
	}
	// float transactions
	{
		if err := createTxnKeyTable(dbconn, l.floatKeyTable); err != nil {
			return err
		}

		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		query := fmt.Sprintf(`create table if not exists "%s" (
			"id" serial primary key,
			"type" varchar(32) not null check("type" in ('put', 'get', 'delete', 'incr', 'incrby')),
			"key_id" integer,
			"value" numeric,
			"timestamp" timestamp not null default now(),
			foreign key("key_id") references "%s"("id") on delete cascade);`, l.floatTxnTable, l.floatKeyTable)

		if _, err := dbconn.Exec(ctx, query); err != nil {
			return fmt.Errorf("failed to create %s table %w", l.floatKeyTable, err)
		}
	}
	// string transactions
	{
		if err := createTxnKeyTable(dbconn, l.strKeyTable); err != nil {
			return err
		}

		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		query := fmt.Sprintf(`create table if not exists "%s" (
			"id" serial primary key,
			"type" varchar(32) not null check("type" in ('put', 'get', 'delete', 'incr', 'incrby')),
			"key_id" integer,
			"value" text,
			"timestamp" timestamp not null default now(),
			foreign key("key_id") references "%s"("id") on delete cascade);`, l.strTxnTable, l.strKeyTable)

		if _, err = dbconn.Exec(ctx, query); err != nil {
			return fmt.Errorf("failed to crete %s table %w", l.strTxnTable, err)
		}
	}
	// map transactions
	{

		if err := createTxnKeyTable(dbconn, l.mapHashTable); err != nil {
			return err
		}

		{
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()

			// type -> transaction type ('put', 'get', 'delete', ...)
			// map_id -> id of the hash which refers to a map to be modified (depending on a transaction type)
			// |---------------------------------|
			// | id | type | map_id | timestamp  |
			// |----|------|--------|------------|
			query := `create table if not exists "map_transactions" (
				"id" serial primary key,
				"type" varchar(32) not null check("type" in ('put', 'get', 'delete', 'incr', 'incrby')),
				"map_id" integer, 
				"timestampt" timestamp not null default now(),
				foreign key("map_id") references "map_hashes"("id") on delete cascade);`

			if _, err = dbconn.Exec(ctx, query); err != nil {
				return fmt.Errorf("failed to create map transactions table %w", err)
			}
		}
		{
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()

			// txn_id -> transaction id ('put', 'get', 'delete', etc.)
			// map_id -> id of key key which holds this map.
			// |-----------------------------------|
			// | txn_id |  map_id |  key |  value  |
			// |--------|---------|------|---------|
			// |  1     |  12     |"bar" | "foo"   |
			// |  1     |  12     | "baz"| "bazzz" |
			query := `create table if not exists "map_key_values" (
				"txn_id" integer,
				"map_id" integer,
				"key" text not null,
				"value" text not null,
				foreign key("map_id") references "map_hashes"("id") on delete cascade,
				foreign key("txn_id") references "map_transactions"("id") on delete cascade);`

			if _, err = dbconn.Exec(ctx, query); err != nil {
				return fmt.Errorf("failed to create %s table %w", l.mapKeyValueTable, err)
			}
		}
	}

	return nil
}

// func (l *PostgresTxnLogger) WriteTransaction(txnType TxnType, storage StorageType, key string, value interface{}) {
// 	// NOTE: Timestamp is used for file transactions only.
// 	// For Postgre we maintain a inserttime column which is default to now().
// 	// So, whenever the transactions is inserted, a timestamp computed automatically.
// 	l.eventsChan <- Event{storageType: storage, txnType: txnType, key: key, value: value}
// }

func extractKeyId(dbconn *pgxpool.Conn, table string, event *Event) (int32, error) {
	rows, _ := dbconn.Query(context.Background(), fmt.Sprintf(`select "id" from "%s" where "key" = ($1);`, table), event.key)
	keyId, err := pgx.CollectOneRow(rows, pgx.RowTo[int32])
	if err != nil { // either `key` doesn't exist, or an error occured
		if err == pgx.ErrNoRows { // `key` doesn't exist
			rows, err = dbconn.Query(context.Background(), fmt.Sprintf(`insert into "%s" ("key") values ($1) returning "id";`, table), event.key)
			keyId, err = pgx.CollectOneRow(rows, pgx.RowTo[int32])
			if err != nil {
				return 0, fmt.Errorf("failed to insert key into %s, %w", table, err)
			}
		} else {
			return 0, fmt.Errorf("failed to select key from %s, %w", table, err)
		}
	}
	return keyId, nil
}

func (l *PostgresTxnLogger) insertTransactionIntoDB(dbconn *pgxpool.Conn, event *Event) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	_ = ctx

	// switch event.storageType {
	// case storageInt:
	// 	keyId, err := extractKeyId(dbconn, l.intKeyTable, event)
	// 	if err != nil {
	// 		return err
	// 	}

	// 	var value interface{} = nil
	// 	if event.txnType == txnPut {
	// 		value = event.value.(int32)
	// 	}
	// 	if _, err = dbconn.Exec(ctx, fmt.Sprintf(`insert into "%s" ("type", "key_id", "value") values ($1, $2, $3);`, l.intTxnTable), event.txnType, keyId, value); err != nil {
	// 		return fmt.Errorf("failed to insert into %s table %w", l.intTxnTable, err)
	// 	}
	// 	log.Logger.Info("Inserted event %v into int table", event)

	// case storageFloat:
	// 	keyId, err := extractKeyId(dbconn, l.floatKeyTable, event)
	// 	if err != nil {
	// 		return err
	// 	}

	// 	var value interface{} = nil
	// 	if event.txnType == txnPut {
	// 		value = event.value.(float32)
	// 	}
	// 	if _, err = dbconn.Exec(ctx, fmt.Sprintf(`insert into "%s" ("type", "key_id", "value") values ($1, $2, $3);`, l.floatTxnTable), event.txnType, keyId, value); err != nil {
	// 		return fmt.Errorf("failed to insert into %s table %w", l.floatTxnTable, err)
	// 	}
	// 	log.Logger.Info("Inserted event %v into float table", event)

	// case storageString:
	// 	keyId, err := extractKeyId(dbconn, l.strKeyTable, event)
	// 	if err != nil {
	// 		return err
	// 	}

	// 	var value interface{} = nil
	// 	if event.txnType == txnPut {
	// 		value = event.value.(string)
	// 	}
	// 	if _, err := dbconn.Exec(ctx, fmt.Sprintf(`insert into "%s" ("type", "key_id", "value") values ($1, $2, $3);`, l.strTxnTable), event.txnType, keyId, value); err != nil {
	// 		return fmt.Errorf("failed to insert into %s table %w", l.strTxnTable, err)
	// 	}
	// 	log.Logger.Info("Inserted event %v into string table", event)

	// case storageMap:
	// 	hashId, err := extractKeyId(dbconn, l.mapHashTable, event)
	// 	if err != nil {
	// 		return err
	// 	}
	// 	rows, _ := dbconn.Query(ctx, `insert into "map_transactions" ("type", "map_id") values ($1, $2) returning "id";`, event.txnType, hashId)
	// 	txnId, err := pgx.CollectOneRow(rows, pgx.RowTo[int32])
	// 	if err != nil {
	// 		return fmt.Errorf("failed to insert into %s table %w", l.mapTxnTable, err)
	// 	}

	// 	// If the event contains data, insert that data into key-value table.
	// 	// Queue all the queries and send them in a single batch.
	// 	if event.txnType == txnPut {
	// 		batch := &pgx.Batch{}
	// 		for key, value := range event.value.(map[string]string) {
	// 			batch.Queue(`insert into "map_key_values" ("txn_id", "map_id", "key", "value") values ($1, $2, $3, $4);`, txnId, hashId, key, value)
	// 		}
	// 		err = dbconn.SendBatch(context.Background(), batch).Close()
	// 		if err != nil {
	// 			return fmt.Errorf("failed to insert into %s table %w", l.mapKeyValueTable, err)
	// 		}
	// 	}
	// 	log.Logger.Info("Inserted event %v into map table", event)
	// }

	return nil
}

func (l *PostgresTxnLogger) Close() {
	l.dbpool.Close()
}

func (l *PostgresTxnLogger) ProcessTransactions(ctx context.Context) {
	// NOTE: We never read errors sent here.
	// There should be a goroutine running on the background and and processing
	// incoming errors. If we encounter at least one while writing transactions,
	// we should stop the server?!
	l.wg.Add(1)
	go func() {
		defer l.wg.Done()

		dbconn, err := l.dbpool.Acquire(context.Background())
		if err != nil {
			l.errorsChan <- fmt.Errorf("failed to acquire db connection %w", err)
			return
		}

		// Release the acquired connection first, before closing the database connection.
		defer dbconn.Release()

		for {
			select {
			case event := <-l.eventsChan:
				if err := l.insertTransactionIntoDB(dbconn, &event); err != nil {
					log.Logger.Error("Failed to insert an event %v, %v", event, err)
					l.errorsChan <- err
					return
				}

			case <-ctx.Done():
				if len(l.eventsChan) > 0 {
					for event := range l.eventsChan {
						if err := l.insertTransactionIntoDB(dbconn, &event); err != nil {
							l.errorsChan <- err
							return
						}
					}
				}
				return
			}
		}
	}()
}

// func selectEventsFromTable(dbconn *pgxpool.Conn, storage StorageType, query, txnTable, keyTable string, eventsChan chan<- Event) error {
// 	// We can retrieve all the events in two ways here.
// 	// The first one is to get all the ids first from `keys` table.
// 	// Then use `inner join` to get all transactions with the specified id in a form of ("type", "key", "value")
// 	//
// 	// Second approach is faster and more robust, but for the sake of practice, I will go with joins.
// 	// Get all pairs ("id", "key").
// 	// Iterate over every id and retrieve all transactions for it.
// 	// Since we know the key already, we would have to create events manually
// 	var keyIds []int32
// 	var err error

// 	timeout := 15 * time.Second

// 	{
// 		ctx, cancel := context.WithTimeout(context.Background(), timeout)
// 		defer cancel()

// 		rows, _ := dbconn.Query(ctx, fmt.Sprintf(`select "id" from "%s";`, keyTable))
// 		keyIds, err = pgx.CollectRows(rows, pgx.RowTo[int32])
// 		if err != nil {
// 			return fmt.Errorf("failed to select rows for %s table, %w", keyTable, err)
// 		}
// 	}

// 	// Sending all the queries in a single batch.
// 	if len(keyIds) != 0 {
// 		batch := &pgx.Batch{}
// 		for _, id := range keyIds {
// 			batch.Queue(query, id).Query(func(rows pgx.Rows) error {
// 				events, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (Event, error) {
// 					// NOTE: For some reason we query more events than we ever insert.
// 					// Which makese me think that we use batching incorrectly.
// 					event := Event{storageType: storage}
// 					err := row.Scan(&event.txnType, &event.key, &event.value, &event.timestamp)
// 					return event, err
// 				})
// 				if err != nil {
// 					log.Logger.Error("Error occured when reading events from %s table, %v", txnTable, err)
// 					return err
// 				}
// 				for _, event := range events {
// 					eventsChan <- event
// 				}
// 				return nil
// 			})
// 		}
// 		ctx, cancel := context.WithTimeout(context.Background(), timeout)
// 		defer cancel()

// 		err = dbconn.SendBatch(ctx, batch).Close()
// 		if err != nil {
// 			return fmt.Errorf("failed to select events from %s table, %w", txnTable, err)
// 		}
// 	}

// 	return nil
// }

func (l *PostgresTxnLogger) ReadEvents() (<-chan Event, <-chan error) {
	readEventsChan := make(chan Event)
	discoveredErrorsChan := make(chan error, 1)

	go func() {
		defer close(readEventsChan)
		defer close(discoveredErrorsChan)

		dbconn, err := l.dbpool.Acquire(context.Background())
		if err != nil {
			discoveredErrorsChan <- fmt.Errorf("failed to acquire db connection %w", err)
			return
		}
		defer dbconn.Release()

		// Passing the query string directly to make the code more explicit
		query := `select "type", "key", "value", "timestamp" from "int_transactions"
			join "int_keys" on "int_keys"."id" = "int_transactions"."key_id"
			where "int_keys"."id" = ($1);`

		// if err := selectEventsFromTable(dbconn, storageInt, query, l.intTxnTable, l.intKeyTable, readEventsChan); err != nil {
		// 	discoveredErrorsChan <- err
		// 	return
		// }

		// do the type cast from `numeric` type to real(float32)
		query = `select "type", "key", cast("value" as real), "timestamp" from "float_transactions"
			join "float_keys" on "float_keys"."id" = "float_transactions"."key_id"
			where "float_keys"."id" = ($1);`

		// if err := selectEventsFromTable(dbconn, storageFloat, query, l.floatTxnTable, l.floatKeyTable, readEventsChan); err != nil {
		// 	discoveredErrorsChan <- err
		// 	return
		// }

		query = `select "type", "key", "value", "timestamp" from "str_transactions"
			join "str_keys" on "str_keys"."id" = "str_transactions"."key_id"
			where "str_keys"."id" = ($1);`

		_ = query
		// if err := selectEventsFromTable(dbconn, storageString, query, l.strTxnTable, l.strKeyTable, readEventsChan); err != nil {
		// 	discoveredErrorsChan <- err
		// 	return
		// }

		// {
		// ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		// defer cancel()

		// type Txn struct {
		// 	Id     int32
		// 	Type   TxnType
		// 	HashId int32
		// 	Hash   string
		// }

		// query = `select "map_transactions"."id", "type", "map_hashes"."id" as "hash_id", "map_hashes"."key" as "hash"
		// 		from "map_transactions"
		// 		join "map_hashes" on "map_hashes"."id" = "map_transactions"."map_id";`

		// rows, _ := dbconn.Query(ctx, query)
		// txns, err := pgx.CollectRows(rows, pgx.RowToStructByPos[Txn])
		// if err != nil {
		// 	discoveredErrorsChan <- fmt.Errorf("failed to select map transaction ids %w", err)
		// 	return
		// }

		// type  |     hash      |   key   |       value
		// ------+---------------+---------+-------------------
		//  put  | hash_0xfffa99 | event   | INSERT
		//  put  | hash_0xfffa99 | id      | 1234
		//  put  | hash_0xfffa99 | message | "hello how are you"
		// query = `select "type", "map_hashes"."key" as "hash", "map_key_values"."key", "value" from "map_transactions"
		// 		join "map_hashes" on "map_transactions"."map_id" = "map_hashes"."id"
		// 		join "map_key_values" on "map_transactions"."id" = "map_key_values"."txn_id"
		// 		where "map_transactions"."id" = ($1);`
		// 	if len(txns) != 0 {
		// 		query = `select "key", "value" from "map_key_values" where "txn_id" = ($1) and "map_id" = ($2);`
		// 		batch := &pgx.Batch{}
		// 		for _, txn := range txns {
		// 			if txn.Type == txnPut {
		// 				batch.Queue(query, txn.Id, txn.HashId).Query(func(rows pgx.Rows) error {
		// 					hashmap := make(map[string]string)
		// 					if _, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (map[string]string, error) {
		// 						var key, value string
		// 						err := row.Scan(&key, &value)
		// 						hashmap[key] = value
		// 						return nil, err
		// 					}); err == nil {
		// 						// Send event with the value containing a hashmap
		// 						readEventsChan <- Event{storageType: storageMap, txnType: txn.Type, key: txn.Hash, value: hashmap}
		// 					}
		// 					return err
		// 				})
		// 			} else {
		// 				// NOTE: Since all the get/delete transactions should happen after any put transactions
		// 				// we need to make a batch query event though we won't use the result anyhow.
		// 				// This should be restructured.
		// 				batch.Queue(query, txn.Id, txn.HashId).QueryRow(func(row pgx.Row) error {
		// 					readEventsChan <- Event{storageType: storageMap, txnType: txn.Type, key: txn.Hash}
		// 					return nil
		// 				})
		// 			}
		// 		}

		// 		batchCtx, batchCancel := context.WithTimeout(context.Background(), 15*time.Second)
		// 		defer batchCancel()

		// 		if err := dbconn.SendBatch(batchCtx, batch).Close(); err != nil {
		// 			discoveredErrorsChan <- fmt.Errorf("failed to query map key value table %w", err)
		// 			return
		// 		}
		// 	}
		// }
	}()

	return readEventsChan, discoveredErrorsChan
}
