package txn

import (
	"context"
	"fmt"
	"io"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/isnastish/kvs/pkg/apitypes"
	"github.com/isnastish/kvs/pkg/log"
)

type TransactionLogger interface {
	ReadTransactions() (<-chan *apitypes.Transaction, <-chan error)
	WriteTransaction(*apitypes.Transaction)
	HandleTransactions() <-chan error
}

type FileTransactionLogger struct {
	// TODO: Implement
}

type PostgresTransactionLogger struct {
	connPool     *pgxpool.Pool
	transactChan chan *apitypes.Transaction
}

type ServiceSettings struct {
}

type TransactionService struct {
	TransactionLogger
}

func NewTransactionService(logger TransactionLogger) *TransactionService {
	return &TransactionService{
		TransactionLogger: logger,
	}
}

func NewFileTransactionLogger(filePath string) (*FileTransactionLogger, error) {
	// TODO: Not implemented yet.
	return nil, nil
}

func NewPostgresTransactionLogger() (*PostgresTransactionLogger, error) {
	// TODO: Get databse URL from the environment
	dbConfig, err := pgxpool.ParseConfig("postgresql://postgres:nastish@postgres-db:5432/postgres?sslmode=disable")
	if err != nil {
		return nil, fmt.Errorf("failed to build a config %v", err)
	}

	dbpool, err := pgxpool.NewWithConfig(context.Background(), dbConfig)
	if err != nil {
		return nil, err
	}

	if err := dbpool.Ping(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to establish db connection %v", err)
	}

	logger := &PostgresTransactionLogger{
		connPool:     dbpool,
		transactChan: make(chan *apitypes.Transaction),
	}

	if err := logger.createTables(); err != nil {
		log.Logger.Error("Failed to create tables %v", err)
		return nil, err
	}

	return logger, nil
}

func (l *PostgresTransactionLogger) createTables() error {
	conn, err := l.connPool.Acquire(context.Background())
	if err != nil {
		return fmt.Errorf("failed to acquire database connection %v", err)
	}
	defer conn.Release()

	// Int table
	{
		if _, err := conn.Exec(context.Background(),
			`CREATE TABLE IF NOT EXISTS "int_keys" (
			"id" SERIAL, 
			"key" TEXT NOT NULL UNIQUE, 
			PRIMARY KEY("id"));`); err != nil {

			return fmt.Errorf("failed to create int keys table %v", err)
		}

		if _, err := conn.Exec(context.Background(),
			`CREATE TABLE IF NOT EXISTS "integer_transactions" (
			"id" SERIAL,
			"transaction_type" CHARACTER VARYING(32) NOT NULL,
			"key_id" SERIAL,
			"value" INTEGER,
			"timestamp" TIMESTAMP NOT NULL DEFAULT NOW(),
			PRIMARY KEY("id"),
			FOREIGN KEY("key_id") REFERENCES "int_keys"("id") ON DELETE CASCADE);`); err != nil {

			return fmt.Errorf("failed to create integer transactions table %v", err)
		}
	}

	// Uint table
	{
		if _, err := conn.Exec(context.Background(),
			`CREATE TABLE IF NOT EXISTS "uint_keys" (
			"id" SERIAL, 
			"key" TEXT NOT NULL UNIQUE,
			PRIMARY KEY("id"));`); err != nil {

			return fmt.Errorf("failed to create uint keys table %v", err)
		}

		if _, err := conn.Exec(context.Background(),
			`CREATE TABLE IF NOT EXISTS "uint_transactions" (
			"id" SERIAL, 
			"transaction_type" CHARACTER VARYING(32) NOT NULL, 
			"key_id" SERIAL,
			"value" SERIAL,
			"timestamp" TIMESTAMP NOT NULL DEFAULT NOW(),
			PRIMARY KEY("id"),
			FOREIGN KEY("key_id") REFERENCES "uint_keys"("id") ON DELETE CASCADE);`); err != nil {

			return fmt.Errorf("failed to create uint transactions table %v", err)
		}
	}

	// Floats table
	{
		if _, err := conn.Exec(context.Background(),
			`CREATE TABLE IF NOT EXISTS "float_keys" (
			"id" SERIAL, 
			"key" TEXT NOT NULL UNIQUE,
			PRIMARY KEY("id"));`); err != nil {

			return fmt.Errorf("failed to create float keys table %v", err)
		}

		if _, err := conn.Exec(context.Background(),
			`CREATE TABLE IF NOT EXISTS "float_transactions" (
			"id" SERIAL,
			"transaction_type" CHARACTER VARYING(32) NOT NULL, 
			"key_id" SERIAL,
			"value" REAL,
			"timestamp" TIMESTAMP NOT NULL DEFAULT NOW(),
			PRIMARY KEY("id"),
			FOREIGN KEY("key_id") REFERENCES "float_keys"("id") ON DELETE CASCADE);`); err != nil {

			return fmt.Errorf("failed to create float transactions table %v", err)
		}
	}

	// String table
	{
		if _, err := conn.Exec(context.Background(),
			`CREATE TABLE IF NOT EXISTS "string_keys" (
			"id" SERIAL, 
			"key" TEXT NOT NULL UNIQUE,
			PRIMARY KEY("id"));`); err != nil {

			return fmt.Errorf("failed to create string keys table %v", err)
		}

		if _, err := conn.Exec(context.Background(),
			`CREATE TABLE IF NOT EXISTS "string_transactions" (
			"id" SERIAL,
			"transaction_type" CHARACTER VARYING(32) NOT NULL, 
			"key_id" SERIAL, 
			"value" TEXT, 
			"timestamp" TIMESTAMP NOT NULL DEFAULT NOW(),
			PRIMARY KEY("id"),
			FOREIGN KEY("key_id") REFERENCES "string_keys"("id") ON DELETE CASCADE);`); err != nil {

			return fmt.Errorf("failed to create string transactions table %v", err)
		}
	}

	// Map table
	{
		if _, err := conn.Exec(context.Background(),
			`CREATE TABLE IF NOT EXISTS "map_keys" (
			"id" SERIAL,
			"key" TEXT NOT NULL UNIQUE,
			PRIMARY KEY("id"));`); err != nil {

			return fmt.Errorf("failed to create map keys table %v", err)
		}

		if _, err := conn.Exec(context.Background(),
			`CREATE TABLE IF NOT EXISTS "map_transactions" (
			"id" SERIAL,
			"transaction_type" CHARACTER VARYING(32) NOT NULL, 
			"key_id" SERIAL,
			"timestampt" TIMESTAMP NOT NULL DEFAULT NOW(),
			PRIMARY KEY("id"),
			FOREIGN KEY("key_id") REFERENCES "map_keys"("id") ON DELETE CASCADE);`); err != nil {

			return fmt.Errorf("failed to create map transactions table %v", err)
		}

		if _, err := conn.Exec(context.Background(),
			`CREATE TABLE IF NOT EXISTS "map_key_value_pairs" (
			"transaction_id" SERIAL NOT NULL,
			"map_key_id" SERIAL NOT NULL,
			"key" TEXT NOT NULL,
			"value" TEXT NOT NULL,
			FOREIGN KEY("map_key_id") REFERENCES "map_keys"("id"),
			FOREIGN KEY("transaction_id") REFERENCES "map_transactions"("id"));`); err != nil {

			return fmt.Errorf("failed to create map key-value pairs table %v", err)
		}
	}

	return nil
}

func (l *PostgresTransactionLogger) readTransactions(dbConn *pgxpool.Conn, dbQuery string, transactChan chan<- *apitypes.Transaction, transactStorageType apitypes.TransactionStorageType) error {
	rows, _ := dbConn.Query(context.Background(), dbQuery)
	transactions, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*apitypes.Transaction, error) {
		var (
			transact *apitypes.Transaction = &apitypes.Transaction{StorageType: transactStorageType}
			txnType  string
		)
		err := row.Scan(&transact.Timestamp, &txnType, &transact.Key, &transact.Data)
		if err != nil {
			transact.TxnType = apitypes.TransactionType(apitypes.TransactionTypeValue[txnType])
		}
		return transact, err
	})

	if err != nil {
		return err
	}

	for _, transact := range transactions {
		transactChan <- transact
	}

	return nil
}

func (l *PostgresTransactionLogger) ReadTransactions() (<-chan *apitypes.Transaction, <-chan error) {
	transactionChan := make(chan *apitypes.Transaction)
	errorChan := make(chan error) // only 1 error?

	go func() {
		defer close(transactionChan)
		defer close(errorChan)

		dbConn, err := l.connPool.Acquire(context.Background())
		if err != nil {
			errorChan <- fmt.Errorf("failed to acquire database connection from the poool %v", err)
			return
		}
		defer dbConn.Release()

		// Query Int transactions
		{
			query := `SELECT "timestamp", "type", "key", "value"
				FROM "integer_transactions" JOIN "int_keys" ON "int_keys"."id" = "integer_transactions"."key_id"
				WHERE "integer_transactions"."key_id" IN (
					SELECT "id" FROM "int_keys"
				);`

			err := l.readTransactions(dbConn, query, transactionChan, apitypes.StorageInt)
			if err != nil {
				log.Logger.Error("Failed to read int transactions %v", err)
				errorChan <- fmt.Errorf("Failed to read int transactions %v", err)
				return
			}
		}

		// Query Uint transactions
		{
			query := `SELECT "timestamp", "transaction_type", "key", "value"
				FROM "uint_transactions" JOIN "uint_keys" ON "uint_keys"."id" = "key_id"
				WHERE "key_id" IN (
					SELECT "id" FROM "uint_keys"
				);`

			err := l.readTransactions(dbConn, query, transactionChan, apitypes.StorageUint)
			if err != nil {
				log.Logger.Error("Failed to read uint transactions %v", err)
				errorChan <- fmt.Errorf("Failed to read uint transactions %v", err)
				return
			}
		}

		// Query float transactions
		{
			query := `SELECT "timestamp", "type", "key", "value"
				FROM "float_transactions" JOIN "float_keys" ON "float_keys"."id" = "key_id"
				WHERE "key_id" IN (
					SELECT "id" FROM "float_keys"
				);`

			err := l.readTransactions(dbConn, query, transactionChan, apitypes.StorageFloat)
			if err != nil {
				log.Logger.Error("Failed to read float transactions %v", err)
				errorChan <- fmt.Errorf("Failed to read float transactions %v", err)
				return
			}
		}

		// Query String transactions
		{
			query := `SELECT "timestamp", "type", "key", "value" 
				FROM "string_transactions" JOIN "string_keys" ON "string_keys"."id" = "key_id"
				WHERE "key_id" IN (
					SELECT "id" FROM "string_keys"
				);`

			err := l.readTransactions(dbConn, query, transactionChan, apitypes.StorageFloat)
			if err != nil {
				log.Logger.Error("Failed to read string transactions %v", err)
				errorChan <- fmt.Errorf("Failed to read string transactions %v", err)
				return
			}
		}

		// Query Map transactions
		{
			//  TODO: Read map transactions
		}

		// Make a signal that we're done with reading transactions.
		errorChan <- io.EOF
	}()

	return transactionChan, errorChan
}

func (l *PostgresTransactionLogger) WriteTransaction(transact *apitypes.Transaction) {
	l.transactChan <- transact
}

func (l *PostgresTransactionLogger) insertTransactionKey(dbConn *pgxpool.Conn, transact *apitypes.Transaction, table string) (*int32, error) {
	// Get key's id from the table if exists, if it doesn't, insert it into a table and get its id
	query := fmt.Sprintf(`SELECT "id" FROM "%s" WHERE "key" = ($1)`, table)
	rows, _ := dbConn.Query(context.Background(), query, transact.Key)
	keyId, err := pgx.CollectOneRow(rows, pgx.RowTo[int32])
	if err != nil {
		if err == pgx.ErrNoRows {
			// Insert new key into keys table and return an id
			query := fmt.Sprintf(`INSERT INTO "%s" ("key") values ($1) RETURNING "id";`, table)
			rows, err = dbConn.Query(context.Background(), query, transact.Key)
			keyId, err = pgx.CollectOneRow(rows, pgx.RowTo[int32])
			if err != nil {
				return nil, fmt.Errorf("failed to insert key into a table %s, %v", table, err)
			}
		} else {
			return nil, fmt.Errorf("failed to retrive key id from a table %s, %v", table, err)
		}
	}

	return &keyId, nil
}

func (l *PostgresTransactionLogger) HandleTransactions() <-chan error {
	errorChan := make(chan error)

	go func() {
		dbConn, err := l.connPool.Acquire(context.Background())
		if err != nil {
			errorChan <- fmt.Errorf("failed to acquire database connection from pool %v", err)
			return
		}
		defer dbConn.Release()

		for {
			select {
			case transact := <-l.transactChan:
				switch transact.StorageType {
				case apitypes.StorageInt:
					keyId, err := l.insertTransactionKey(dbConn, transact, "int_keys")
					if err != nil {
						log.Logger.Error("Failed to retrieve transaction key id %v", err)
						errorChan <- err
						return
					}

					_, err = dbConn.Exec(context.Background(),
						`INSERT INTO "integer_transactions" ("timestamp", "transaction_type", "ket_id", "value")
						VALUES ($1, $2, $3, $4);`,
						transact.Timestamp,
						apitypes.TransactionTypeName[int32(transact.TxnType)],
						*keyId,
						transact.Data.(int32),
					)

					if err != nil {
						log.Logger.Error("Failed to write a int transaction %v", err)
						errorChan <- fmt.Errorf("failed to write int transaction %v", err)
						return
					}

				case apitypes.StorageUint:
					keyId, err := l.insertTransactionKey(dbConn, transact, "uint_keys")
					if err != nil {
						log.Logger.Error("Failed to retrieve transaction key id %v", err)
						errorChan <- err
						return
					}

					_, err = dbConn.Exec(context.Background(),
						`INSERT INTO "uint_transactions" ("timestamp", "transaction_type", "ket_id", "value")
						VALUES ($1, $2, $3, $4);`,
						transact.Timestamp,
						apitypes.TransactionTypeName[int32(transact.TxnType)],
						*keyId,
						transact.Data.(uint32),
					)

					if err != nil {
						log.Logger.Error("Failed to write a uint transaction %v", err)
						errorChan <- fmt.Errorf("failed to write uint transaction %v", err)
						return
					}

				case apitypes.StorageFloat:
					keyId, err := l.insertTransactionKey(dbConn, transact, "float_keys")
					if err != nil {
						log.Logger.Error("Failed to retrieve transaction key id %v", err)
						errorChan <- err
						return
					}

					_, err = dbConn.Exec(context.Background(),
						`INSERT INTO "float_transactions" ("timestamp", "transaction_type", "ket_id", "value")
						VALUES ($1, $2, $3, $4);`,
						transact.Timestamp,
						apitypes.TransactionTypeName[int32(transact.TxnType)],
						*keyId,
						transact.Data.(float32),
					)

					if err != nil {
						log.Logger.Error("Failed to write a float transaction %v", err)
						errorChan <- fmt.Errorf("failed to write float transaction %v", err)
						return
					}

				case apitypes.StorageString:
					keyId, err := l.insertTransactionKey(dbConn, transact, "string_keys")
					if err != nil {
						log.Logger.Error("Failed to retrieve transaction key id %v", err)
						errorChan <- err
						return
					}

					_, err = dbConn.Exec(context.Background(),
						`INSERT INTO "string_transactions" ("timestamp", "transaction_type", "ket_id", "value")
						VALUES ($1, $2, $3, $4);`,
						transact.Timestamp,
						apitypes.TransactionTypeName[int32(transact.TxnType)],
						*keyId,
						transact.Data.(float32),
					)

					if err != nil {
						log.Logger.Error("Failed to write a string transaction %v", err)
						errorChan <- fmt.Errorf("failed to write string transaction %v", err)
						return
					}

				case apitypes.StorageMap:
					keyId, err := l.insertTransactionKey(dbConn, transact, "map_keys")
					if err != nil {
						log.Logger.Error("Failed to retrieve transaction key id %v", err)
						errorChan <- err
						return
					}

					// NOTE: Maybe it's possible to use nested insert
					rows, _ := dbConn.Query(context.Background(),
						`INSERT INTO "map_transactions" ("timestamp", "transaction_type", "key_id") VALUES ($1, $2, $3) RETURNING "id";`,
						transact.Timestamp,
						apitypes.TransactionTypeName[int32(transact.TxnType)],
						*keyId,
					)
					transactId, err := pgx.CollectOneRow(rows, pgx.RowTo[int32])
					if err != nil {
						log.Logger.Error("Failed to query transaction id %v", err)
						errorChan <- fmt.Errorf("failed to query transaction id %v", err)
						return
					}

					if transact.Data != nil {
						batch := &pgx.Batch{}
						for key, value := range transact.Data.(map[string]string) {
							batch.Queue(
								`INSERT INTO "map_key_value_pairs" ("transaction_id", "map_key_id", "key", "value") 
								VALUES ($1, $2, $3, $4);`,
								transactId, *keyId, transact.Key, key, value)
						}

						err = dbConn.SendBatch(context.Background(), batch).Close()
						if err != nil {
							log.Logger.Error("Failed to insert map key value pairs %v", err)
							errorChan <- fmt.Errorf("failed to insert into map key value pairs table %v", err)
							return
						}
					}
				}
			}
		}
	}()

	return errorChan
}

////////////////////////////////////////////////////////////////////////////////////////////
// This should be moved into its own package

func (l *FileTransactionLogger) ReadTransactions() (<-chan *apitypes.Transaction, <-chan error) {
	return nil, nil
}

func (l *FileTransactionLogger) WriteTransaction(*apitypes.Transaction) {

}

func (l *FileTransactionLogger) HandleTransactions() <-chan error {
	return nil
}
