package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/isnastish/kvs/pkg/log"
	"github.com/isnastish/kvs/proto/api"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

/////////////////////////////////////Transaction logger interface/////////////////////////////////////

type TransactionLogger interface {
	ReadTransaction() (<-chan *api.Transaction, <-chan error)
	WriteTransaction(*api.Transaction)
	HandleTransactions()
}

/////////////////////////////////////Postgres transaction logger/////////////////////////////////////

type PostgresTransactionLogger struct {
	ConnPool *pgxpool.Pool
}

func NewPostgresTransactionLogger() (*PostgresTransactionLogger, error) {
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
		ConnPool: dbpool,
	}

	if err := logger.createTables(); err != nil {
		log.Logger.Error("Failed to create tables %v", err)
		return nil, err
	}

	return logger, nil
}

func (l *PostgresTransactionLogger) createKeysTable(conn *pgxpool.Conn, table string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	query := fmt.Sprintf(`create table if not exists "%s" (
		"id" serial primary key,
		"key" text not null unique);`, table)

	if _, err := conn.Exec(ctx, query); err != nil {
		return fmt.Errorf("failed to create %s table %v", table, err)
	}

	return nil
}

func (l *PostgresTransactionLogger) createTables() error {
	conn, err := l.ConnPool.Acquire(context.Background())
	if err != nil {
		return fmt.Errorf("failed to acquire db connection %v", err)
	}
	defer conn.Release()

	//
	// Int table
	//
	{
		err = l.createKeysTable(conn, "int_keys")
		if err != nil {
			return fmt.Errorf("failed to create int keys table %v", err)
		}

		query := `create table if not exists "integer_transactions" (
			"id" serial primary key,
			"transaction_type" character varying(32) not null,
			"key_id" serial,
			"value" integer,
			"timestamp" timestamp not null default now(),
			foreign key("key_id") references "int_keys"("id") on delete cascade
		);`

		_, err = conn.Exec(context.Background(), query)
		if err != nil {
			return fmt.Errorf("failed to create integer transactions table %v", err)
		}
	}

	//
	// Uint table
	//
	{
		err = l.createKeysTable(conn, "uint_keys")
		if err != nil {
			return fmt.Errorf("failed to create uint keys table %v", err)
		}

		query := `create table if not exists "uint_transactions" (
			"id" serial primary key, 
			"transaction_type" character varying(32) not null, 
			"key_id" serial, 
			"value" serial,
			"timestamp" timestamp not null default now(),
			foreign key("key_id") references "uint_keys"("id") on delete cascade
		);`

		_, err = conn.Exec(context.Background(), query)
		if err != nil {
			return fmt.Errorf("failed to create uint transactions table %v", err)
		}
	}

	//
	// Floats table
	//
	{
		err = l.createKeysTable(conn, "float_keys")
		if err != nil {
			return fmt.Errorf("failed to create float keys table %v", err)
		}

		query := `create table if not exists "float_transactions" (
			"id" serial primary key, 
			"transaction_type" character varying(32) not null, 
			"key_id" serial,
			"value" real,
			"timestamp" timestamp not null default now(),
			foreign key("key_id") references "float_keys"("id") on delete cascade
		);`

		_, err = conn.Exec(context.Background(), query)
		if err != nil {
			return fmt.Errorf("failed to create float transactions table %v", err)
		}
	}

	//
	// String table
	//
	{
		err = l.createKeysTable(conn, "string_keys")
		if err != nil {
			return fmt.Errorf("failed to create string keys table %v", err)
		}

		query := `create table if not exists "string_transactions" (
			"id" serial primary key, 
			"transaction_type" character varying(32) not null, 
			"key_id" serial, 
			"value" text, 
			"timestamp" timestamp not null default now(),
			foreign key("key_id") reference "string_keys"("id") on delete cascade
		);`

		_, err = conn.Exec(context.Background(), query)
		if err != nil {
			return fmt.Errorf("failed to create string transactions table %v", err)
		}
	}

	//
	// Map table
	//
	{
		err = l.createKeysTable(conn, "map_keys")
		if err != nil {
			return fmt.Errorf("failed to create map keys table %v", err)
		}

		query := `create table if not exists "map_transactions" (
			"id" serial primary key, 
			"transaction_type" character varying(32) not null, 
			"key_id" serial,
			"timestampt" timestamp not null default now(),
			foreign key("key_id") references "map_keys"("id") on delete cascade
		);`

		_, err = conn.Exec(context.Background(), query)
		if err != nil {
			return fmt.Errorf("failed to create map transactions table %v", err)
		}

		query = `create table if not exists "map_key_value_pairs" (
			"transaction_id" serial not null,
			"map_key_id" serial not null,
			"key" text not null, 
			"value" text not null,
			foreign key("map_key_id") references "map_keys"("id") on delete cascade,
			foreign key("transaction_id") references "map_transactions"("id") on delete cascade
		);`

		_, err = conn.Exec(context.Background(), query)
		if err != nil {
			return fmt.Errorf("failed to create map key-value pairs table %v", err)
		}
	}

	return nil
}

func (s *PostgresTransactionLogger) queryTransactions(
	conn *pgxpool.Conn, storage api.StorageType,
	transactionTable string, keysTable string, query string) ([]*api.Transaction, error) {

	var result []*api.Transaction

	rows, _ := conn.Query(context.Background(), fmt.Sprintf(`select "id" from "%s";`, keysTable))
	keyIds, err := pgx.CollectRows(rows, pgx.RowTo[int32])
	if err != nil {
		log.Logger.Error("Failed to query key ids from %s table %v", keysTable, err)
		return result, fmt.Errorf("failed to query key ids from %s table, error %v", keysTable, err)
	}

	if len(keyIds) != 0 {
		batch := &pgx.Batch{}
		for _, id := range keyIds {
			batch.Queue(query, id).Query(func(rows pgx.Rows) error {
				transactions, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*api.Transaction, error) {
					var txnType string
					var key string
					var value interface{}

					err := row.Scan(&txnType, &key, &value)
					if err != nil {
						log.Logger.Error("Failed to scan row %v", err)
						return nil, err
					}

					transaction := &api.Transaction{
						StorageType: storage,
						Timestamp:   timestamppb.Now(), // NOTE: We can omit the timestamp
						TxnType:     api.TxnType(api.TxnType_value[txnType]),
						Key:         key,
					}

					// It introduces a huge overhead by invoking a switch statement for each row, since the storage type is known from the beginning
					// Most likely I will be able to use the reflection package somehow to workaround this problem.
					switch storage {
					case api.StorageType_StorageInt:
						transaction.Value = &api.Value{Value: &api.Value_IntValue{IntValue: value.(int32)}}

					case api.StorageType_StorageFloat:
						transaction.Value = &api.Value{Value: &api.Value_FloatValue{FloatValue: value.(float32)}}

					case api.StorageType_StorageUint:
						transaction.Value = &api.Value{Value: &api.Value_UintValue{UintValue: value.(uint32)}}

					case api.StorageType_StorageStr:
						transaction.Value = &api.Value{Value: &api.Value_StrValue{StrValue: value.(string)}}

					case api.StorageType_StorageMap:
						transaction.Value = &api.Value{Value: &api.Value_MapValue{MapValue: &api.Map{Data: value.(map[string]string)}}}
					}

					return transaction, nil
				})

				if err != nil {
					log.Logger.Error("Error occured when reading events from %s table, %v", transactionTable, err)
					return err
				}

				result = append(result, transactions...)

				return nil
			})
		}

		err = conn.SendBatch(context.Background(), batch).Close()
		if err != nil {
			log.Logger.Error("Failed to query transactions from %s table %v", transactionTable, err)
			return result, fmt.Errorf("failed to query transactions from %s table, %v", transactionTable, err)
		}
	}

	return result, nil
}

func (s *PostgresTransactionLogger) ReadTransaction() (<-chan *api.Transaction, <-chan error) {

	transactionChan := make(chan *api.Transaction)
	errorChan := make(chan error) // ,1

	sendTransactions := func(transactions []*api.Transaction) {
		for _, txn := range transactions {
			transactionChan <- txn
		}
	}

	// NOTE: Since all transactions are located in separate tables, we could process them on separate goroutines to speed up the process
	go func() {
		defer close(transactionChan)
		defer close(errorChan)

		conn, err := s.ConnPool.Acquire(context.Background())
		if err != nil {
			errorChan <- fmt.Errorf("failed to acquire db connection from the poool %v", err)
			return
		}
		defer conn.Release()

		//
		// Query Int transactions
		//
		{
			query := `select "type", "key", "value" from "integer_transactions" 
			join "int_keys" on "int_keys"."id" = "integer_transactions"."key_id"
			where "int_keys"."id" = ($1);`

			transactions, err := s.queryTransactions(conn, api.StorageType_StorageInt, "integer_transactions", "int_keys", query)
			if err != nil {
				errorChan <- err
				return // ?
			}

			sendTransactions(transactions)
		}

		//
		// Query Uint transactions
		//
		{
			query := `select "type", "key", "value" from "uint_transactions"
			join "uint_keys" on "uint_keys"."id" = "uint_transactions"."key_id"
			where "uint_keys"."id" = ($1);`

			transactions, err := s.queryTransactions(conn, api.StorageType_StorageUint, "uint_transactions", "uint_keys", query)
			if err != nil {
				errorChan <- err
				return
			}

			sendTransactions(transactions)
		}

		//
		// Query float transactions
		//
		{
			query := `select "type", "key", "value" from "float_transactions"
			join "float_keys" on "float_keys"."id" = "float_transactions"."key_id"
			where "float_keys"."id" = ($1);`

			transactions, err := s.queryTransactions(conn, api.StorageType_StorageFloat, "float_transactions", "float_keys", query)
			if err != nil {
				errorChan <- err
				return
			}

			sendTransactions(transactions)
		}

		//
		// Query String transactions
		//
		{
			query := `select "type", "key", "value" from "string_transactions"
			join "string_keys" on "string_keys"."id" = "string_transactions"."key_id"
			where "string_keys"."id" = ($1);`

			transactions, err := s.queryTransactions(conn, api.StorageType_StorageStr, "string_transactions", "string_keys", query)
			if err != nil {
				errorChan <- err
				return
			}

			sendTransactions(transactions)
		}

		//
		// Query Map transactions
		//
		{
			//  TODO: Handler map transaction querying
		}
	}()

	return transactionChan, errorChan
}

func (s *PostgresTransactionLogger) WriteTransaction(*api.Transaction) {

}

func (s *PostgresTransactionLogger) HandleTransactions() {

}

/////////////////////////////////////transaction service implementation/////////////////////////////////////

type TransactionService struct {
	api.UnimplementedTransactionServiceServer

	logger TransactionLogger
}

func (s *TransactionService) ReadTransactions(_ *emptypb.Empty, stream api.TransactionService_ReadTransactionsServer) error {
	log.Logger.Info("Opened stream for reading transaction")

	// transactionChan, errorChan := s.logger.ReadTransaction()
	// for {
	// 	select {
	// 	case transaction := <-transactionChan:
	// 		err := stream.Send(transaction)
	// 		if err != nil {
	// 			log.Logger.Error("Failed to send a transaction %v", err)
	// 		}

	// 	case err := <-errorChan:
	// 		log.Logger.Error("Error while reading transactions %v", err)
	// 		return nil
	// 	}
	// }

	return nil
}

func (s *TransactionService) WriteTransactions(stream api.TransactionService_WriteTransactionsServer) error {
	log.Logger.Info("Opened stream for writing transactions")
	return nil
}

func (s *TransactionService) ProcessErrors(_ *emptypb.Empty, stream api.TransactionService_ProcessErrorsServer) error {
	log.Logger.Info("Opened stream for processing errors")
	return nil
}

//////////////////////////////////grpc server wrapper//////////////////////////////////

type GRPCServer struct {
	server *grpc.Server
}

func NewGRPCServer() *GRPCServer {
	// TODO: Use service description for registering GRPC services
	return &GRPCServer{
		server: grpc.NewServer(),
	}
}

func (s *GRPCServer) Serve(port uint) error {
	listener, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", port))
	if err != nil {
		log.Logger.Error("grpc: failed to listen %v", err)
		return fmt.Errorf("failed to listen %v", err)
	}

	log.Logger.Info("Listening on port 0.0.0.0:%d", port)

	// Will return a non-nil error unless Stop is called
	err = s.server.Serve(listener)
	if err != nil {
		log.Logger.Error("Failed to serve %v", err)
	}

	return err
}

func (s *GRPCServer) Close() {
	s.server.Stop()
}

func main() {
	/////////////////////////////////////Transaction service port/////////////////////////////////////
	txnPort := flag.Uint("port", 5051, "Transaction service listening port")
	flag.Parse()

	////////////////////////////////////Connect to PostgreSQL database////////////////////////////////////
	// logger, err := NewPostgresTransactionLogger()
	// if err != nil {
	// 	log.Logger.Fatal("Failed to init DB transaction logger %v", err)
	// 	os.Exit(1)
	// }

	log.Logger.Info("Successfully established database connection")

	grpcServer := NewGRPCServer()
	api.RegisterTransactionServiceServer(grpcServer.server, &TransactionService{logger: nil /*logger*/})

	doneChan := make(chan bool, 1)

	go func() {
		defer close(doneChan)
		// TODO: Handle errors
		grpcServer.Serve(*txnPort)
	}()

	osSigChan := make(chan os.Signal, 1)
	signal.Notify(osSigChan, syscall.SIGINT, syscall.SIGTERM)

	<-osSigChan
	grpcServer.Close()
	<-doneChan
}
