package txn

import (
	"context"
	"fmt"

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
	connPool *pgxpool.Pool
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
		connPool: dbpool,
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

	//
	// Int table
	//
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

	//
	// Uint table
	//
	{
		if _, err := conn.Exec(context.Background(),
			`CREATE TABLE IF NOT EXISTS "uint_keys" (
			"id" SERIAL, 
			"key" TEXT NOT NULL UNIQUE,
			PRIMARY KEY("id"));`); err != nil {

			return fmt.Errorf("failed to create uint keys table %v", err)
		}

		if _, err := conn.Exec(context.Background(),
			`CREATE TABLE IF NOT EXIST "uint_transactions" (
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

	//
	// Floats table
	//
	{
		if _, err := conn.Exec(context.Background(),
			`CREATE TABLE IF NOT EXISTS "float_keys" (
			"id" SERIAL, 
			"key" TEXT NOT NULL UNIQUE,
			PRIMARY KEY("id"));`); err != nil {

			return fmt.Errorf("failed to create float keys table %v", err)
		}

		if _, err := conn.Exec(context.Background(),
			`CREATE TABLE IF NOT EXIST "float_transactions" (
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

	//
	// String table
	//
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
			"timestamp" TIMESTAMP NOT NULL DEFAULT NOW()
			PRIMARY KEY("id"),
			FOREIGN KEY("key_id") REFERENCE "string_keys"("id") ON DELETE CASCADE);`); err != nil {

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
			FOREIGN KEY("key_id") REFERENCE "map_keys"("id") ON DELETE CASCADE);`); err != nil {

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
	}()

	return transactionChan, errorChan
}

func (l *PostgresTransactionLogger) WriteTransaction(*apitypes.Transaction) {

}

func (l *PostgresTransactionLogger) HandleTransactions() <-chan error {
	errorChan := make(chan error)

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
