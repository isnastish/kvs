package main

import (
	"crypto/tls"
	"crypto/x509"
	"flag"
	"os"
	"os/signal"
	"syscall"

	myapi "github.com/isnastish/kvs/pkg/api"
	"github.com/isnastish/kvs/pkg/log"
	"github.com/isnastish/kvs/pkg/txn"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func main() {
	txnPort := flag.Uint("port", 5051, "Transaction service listening port")
	loggerBackend := flag.String("backend", "postgres", "Backend for logging transactions [file|postgres]")
	serverPrivateKeyFile := flag.String("private_key_file", "server.key", "Server private RSA key")
	serverPublicKeyFile := flag.String("public_key_file", "server.crt", "Server public X509 key")
	caPublicKeyFile := flag.String("ca_public_key_file", "ca.crt", "Public kye of a CA used to sign all public certificates")

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

	// parse server's public/private keys
	cert, err := tls.LoadX509KeyPair(*serverPublicKeyFile, *serverPrivateKeyFile)
	if err != nil {
		log.Logger.Fatal("Failed to parse public/private key pair %v", err)
		os.Exit(1)
	}

	// create a certificate pool from CA
	certPool := x509.NewCertPool()
	ca, err := os.ReadFile(*caPublicKeyFile)
	if err != nil {
		log.Logger.Fatal("Failed to rea ca certificate %v", err)
		os.Exit(1)
	}

	// append the client certificates from the CA to the certificate pool
	if ok := certPool.AppendCertsFromPEM(ca); !ok {
		log.Logger.Fatal("Failed to append ca certificate %v", err)
		os.Exit(1)
	}

	options := []grpc.ServerOption{
		grpc.Creds(
			credentials.NewTLS(&tls.Config{
				// request client certificate during handshake,
				// and do the validation
				ClientAuth:   tls.RequireAndVerifyClientCert,
				Certificates: []tls.Certificate{cert},
				// root certificate authorities that servers use
				// to verify a client certificate by the policy in ClientAuth
				ClientCAs: certPool,
			}),
		),
	}

	transactionService := txn.NewTransactionService(transactLogger)

	// pass TLS credentials to create                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   secure grpc server
	grpcServer := myapi.NewGRPCServer(myapi.NewTransactionServer(transactionService), options...)

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
