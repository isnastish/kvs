package main

import (
	"flag"
	"os"
	"os/signal"
	"syscall"

	myapi "github.com/isnastish/kvs/pkg/api"
	"github.com/isnastish/kvs/pkg/log"
	"github.com/isnastish/kvs/pkg/txn"
)

func main() {
	txnPort := flag.Uint("port", 5051, "Transaction service listening port")
	loggerBackend := flag.String("backend", "postgres", "Backend for logging transactions [file|postgres]")

	flag.Parse()

	var transactLogger txn.TransactionLogger

	switch *loggerBackend {
	case "postgres":
		postgresLogger, err := txn.NewPostgresTransactionLogger()
		if err != nil {
			log.Logger.Fatal("Failed to create database transaction logger %v", err)
			os.Exit(1)
		}
		transactLogger = postgresLogger

		log.Logger.Info("Successfully connected to database")

	case "file":
		// TODO: Path as a command line argument.
		const filepath = ""
		fileLogger, err := txn.NewFileTransactionLogger(filepath)
		if err != nil {
			log.Logger.Fatal("Failed to create file transaction logger %v", err)
			os.Exit(1)
		}
		transactLogger = fileLogger
	}

	transactionService := txn.NewTransactionService(transactLogger)
	grpcServer := myapi.NewGRPCServer(myapi.NewTransactionServer(transactionService))

	doneChan := make(chan bool, 1)
	osSigChan := make(chan os.Signal, 1)

	signal.Notify(osSigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		defer close(doneChan)
		err := grpcServer.Serve(*txnPort)
		if err != nil {
			log.Logger.Error("Transaction service terminated %v", err)
			close(osSigChan)
		} else {
			log.Logger.Info("Service closed gracefully")
		}
	}()

	<-osSigChan
	grpcServer.Close()
	<-doneChan
}
