package api

import (
	"io"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/isnastish/kvs/pkg/txn"
	"github.com/isnastish/kvs/proto/api"

	"github.com/isnastish/kvs/pkg/apitypes"
	"github.com/isnastish/kvs/pkg/log"
)

type readTransactionsServer interface {
}

type transactionServer struct {
	api.UnimplementedTransactionServiceServer
	service     *txn.TransactionService
	serviceDesc *grpc.ServiceDesc
}

func (s *transactionServer) ServiceDesc() *grpc.ServiceDesc {
	return s.serviceDesc
}

func NewTransactionServer(service *txn.TransactionService) *transactionServer {
	return &transactionServer{
		service:     service,
		serviceDesc: &api.TransactionService_ServiceDesc,
	}
}

func (s *transactionServer) ReadTransactions(_ *emptypb.Empty, stream api.TransactionService_ReadTransactionsServer) error {
	transactionChan, errorChan := s.service.ReadTransactions()
	for {
		select {
		case transact := <-transactionChan:
			var transactionData *api.TransactionData

			if transact.Data != nil {
				switch transact.StorageType {
				case apitypes.StorageInt:
					transactionData = &api.TransactionData{
						Kind: &api.TransactionData_IntValue{IntValue: transact.Data.(int32)},
					}

				case apitypes.StorageUint:
					transactionData = &api.TransactionData{
						Kind: &api.TransactionData_UintValue{UintValue: transact.Data.(uint32)},
					}

				case apitypes.StorageFloat:
					transactionData = &api.TransactionData{
						Kind: &api.TransactionData_FloatValue{FloatValue: transact.Data.(float32)},
					}

				case apitypes.StorageString:
					transactionData = &api.TransactionData{
						Kind: &api.TransactionData_MapValue{
							MapValue: &api.Map{Data: transact.Data.(map[string]string)},
						},
					}
				}
			} else {
				// TODO: Handle NullValue
				transactionData = &api.TransactionData{}
			}

			protoTransaction := &api.Transaction{
				TxnType:     api.TxnType(transact.TxnType),
				StorageType: api.StorageType(transact.StorageType),
				Timestamp:   timestamppb.New(transact.Timestamp),
				Key:         transact.Key,
				Data:        transactionData,
			}

			err := stream.Send(protoTransaction)
			if err != nil {
				log.Logger.Error("Failed to send transaction %v", err)
				return err
			}

		case err := <-errorChan:
			if err == io.EOF {
				log.Logger.Info("Done reading transactions")
				return nil
			}
			return err
		}
	}
}

func (s *transactionServer) WriteTransactions(stream api.TransactionService_WriteTransactionsServer) error {
	serviceErrorChan := s.service.HandleTransactions()

	receivedTransactionChan := make(chan *api.Transaction)
	streamErrorChan := make(chan error) // 1?

	go func() {
		defer close(streamErrorChan)
		for {
			transaction, err := stream.Recv()
			if err != nil {
				streamErrorChan <- err
				return
			}
			receivedTransactionChan <- transaction
		}
	}()

	for {
		select {
		case protoTransaction := <-receivedTransactionChan:
			// Value of the transaction received from the client can be nil,
			// we would have to inspect protoTransaction.Value in order to verify that.
			var apiData interface{}

			if protoTransaction.TxnType == api.TxnType_TxnPut {
				switch protoTransaction.StorageType {
				case api.StorageType_StorageInt:
					apiData = protoTransaction.Data.GetIntValue()

				case api.StorageType_StorageUint:
					apiData = protoTransaction.Data.GetUintValue()

				case api.StorageType_StorageFloat:
					apiData = protoTransaction.Data.GetFloatValue()

				case api.StorageType_StorageStr:
					apiData = protoTransaction.Data.GetStrValue()

				case api.StorageType_StorageMap:
					apiData = protoTransaction.Data.GetMapValue().Data
				}
				// TODO: Consider NullValue as well.
			}

			transaction := &apitypes.Transaction{
				TxnType:     apitypes.TransactionType(protoTransaction.TxnType),
				StorageType: apitypes.TransactionStorageType(protoTransaction.StorageType),
				Timestamp:   protoTransaction.Timestamp.AsTime(),
				Key:         protoTransaction.Key,
				Data:        apiData,
			}

			s.service.WriteTransaction(transaction)

		case err := <-streamErrorChan:
			log.Logger.Error("Failed to receive transaction %v", err)
			return err

		case err := <-serviceErrorChan:
			log.Logger.Error("Failed to process incoming transactions %v", err)
			return err
		}
	}
}
