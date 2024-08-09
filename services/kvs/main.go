package main

import (
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/isnastish/kvs/pkg/kvs"
	"github.com/isnastish/kvs/pkg/log"
	"github.com/isnastish/kvs/proto/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func main() {
	var settings kvs.ServiceSettings

	flag.StringVar(&settings.Endpoint, "endpoint", ":8080", "Address to run a key-value storage on")
	flag.BoolVar(&settings.TxnDisabled, "txn_disabled", false, "Disable transactions. Enabled by default")
	logLevel := flag.String("log_level", "debug", "Set global logging level. Feasible values are: (debug|info|warn|error|fatal|panic|disabled)")
	txnPort := flag.Uint("txn_port", 5051, "Transaction service listening port")
	clientPrivateKeyFile := flag.String("public_key_file", "client.key", "File containing cient private RSA key")
	clientPublicKeyFile := flag.String("public_key_file", "client.crt", "File containing client public X509 key")
	caPublicKeyFile := flag.String("ca_public_key_file", "ca.crt", "Public kye of a CA used to sign all public certificates")

	flag.Parse()

	log.SetupGlobalLogLevel(*logLevel)

	//////////////////////////////////////gRPC client//////////////////////////////////////
	// NOTE: This should be an address instead of a container name
	cert, err := tls.LoadX509KeyPair(*clientPublicKeyFile, *clientPrivateKeyFile)
	if err != nil {
		log.Logger.Fatal("Failed to load public/private keys pair %v", err)
		os.Exit(1)
	}

	// Create certificate pool from the CA
	certPool := x509.NewCertPool()
	ca, err := os.ReadFile(*caPublicKeyFile)
	if err != nil {
		log.Logger.Fatal("Failed to read ca certificate %v", err)
		os.Exit(1)
	}

	if ok := certPool.AppendCertsFromPEM(ca); !ok {
		log.Logger.Fatal("Failed to append ca certificate %v", err)
		os.Exit(1)
	}

	options := []grpc.DialOption{
		grpc.WithTransportCredentials(
			credentials.NewTLS(&tls.Config{
				// NOTE: Server name should be equal to Common Name on the certificate
				ServerName:   "localhost",
				Certificates: []tls.Certificate{cert},
				RootCAs:      certPool,
			}),
		),
	}

	txnServiceAddr := fmt.Sprintf("transaction-service:%d", *txnPort)
	grpcClient, err := grpc.NewClient(txnServiceAddr, options...)
	if err != nil {
		log.Logger.Fatal("Failed to create grpc client %v", err)
		os.Exit(1)
	}
	// NOTE: Should we defer closing the connection?
	defer grpcClient.Close()

	txnClient := api.NewTransactionServiceClient(grpcClient)

	kvsService := kvs.NewService(&settings, txnClient)

	doneChan := make(chan bool, 1)
	osSigChan := make(chan os.Signal, 1)

	signal.Notify(osSigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		defer close(doneChan)
		err := kvsService.Run()
		if err != nil {
			log.Logger.Error("Service terminated with an error %v", err)
			close(osSigChan)
		} else {
			log.Logger.Info("Service shut down gracefully")
		}
	}()

	<-osSigChan
	kvsService.Close()
	<-doneChan
}
