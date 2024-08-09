package kvs

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"
	"unicode"

	"github.com/gorilla/mux"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/isnastish/kvs/pkg/apitypes"
	"github.com/isnastish/kvs/pkg/log"
	"github.com/isnastish/kvs/pkg/serviceinfo"
	"github.com/isnastish/kvs/proto/api"
)

// TODO: Implement a throttle pattern on the server side.
// We should limit the amount of requests a client can make to a service
// to 10 requests per second.

// NOTE: Instead of passing nil when making GET/DELETE transactions, introduce separate functions
// writeGetTransaction(), writeDeleteTransaction() and only pass the information, (key and a storageType)

type ServiceSettings struct {
	Endpoint    string
	CertPemFile string
	KeyPemFile  string
	Username    string
	Password    string
	TxnDisabled bool
	TxnLogger
}

type HandlerCallback func(w http.ResponseWriter, req *http.Request)

type RPCHandler struct {
	method   string
	funcName string
	cb       HandlerCallback
}

type Service struct {
	*http.Server
	settings    *ServiceSettings
	rpcHandlers []*RPCHandler
	storage     map[apitypes.TransactionStorageType]Storage
	txnLogger   TxnLogger // TODO: Remove, once we fully transition to using transaction service
	running     bool

	txnClient api.TransactionServiceClient

	transactChan chan *apitypes.Transaction
}

func (s *Service) stringPutHandler(w http.ResponseWriter, req *http.Request) {
	logOnEndpointHit(req.RequestURI, req.Method, req.RemoteAddr)

	key := mux.Vars(req)["key"]
	val, err := io.ReadAll(req.Body)
	defer req.Body.Close()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	cmd := s.storage[apitypes.StorageString].Put(key, newCmdResult(string(val)))
	if cmd.err != nil {
		http.Error(w, cmd.err.Error(), http.StatusInternalServerError)
		return
	}

	s.sendTransaction(&apitypes.Transaction{
		Timestamp:   time.Now(),
		TxnType:     apitypes.TransactionPut,
		StorageType: apitypes.StorageString,
		Key:         key,
		Data:        string(val),
	})

	w.WriteHeader(http.StatusOK)
}

func (s *Service) stringGetHandler(w http.ResponseWriter, req *http.Request) {
	logOnEndpointHit(req.RequestURI, req.Method, req.RemoteAddr)

	key := mux.Vars(req)["key"]
	cmd := s.storage[apitypes.StorageString].Get(key, newCmdResult())
	if cmd.err != nil {
		http.Error(w, cmd.err.Error(), http.StatusNotFound)
		return
	}

	s.sendTransaction(&apitypes.Transaction{
		Timestamp:   time.Now(),
		TxnType:     apitypes.TransactionGet,
		StorageType: apitypes.StorageString,
		Key:         key,
	})

	bytes := []byte(cmd.result.(string))
	w.Header().Add("Content-Type", "text/plain")
	w.Header().Add("Content-Length", strconv.Itoa(len(bytes)))

	w.WriteHeader(http.StatusOK)
	w.Write(bytes)
}

func (s *Service) stringDeleteHandler(w http.ResponseWriter, req *http.Request) {
	logOnEndpointHit(req.RequestURI, req.Method, req.RemoteAddr)

	key := mux.Vars(req)["key"]
	cmd := s.storage[apitypes.StorageString].Del(key, newCmdResult())
	if cmd.result.(bool) {
		s.sendTransaction(&apitypes.Transaction{
			Timestamp:   time.Now(),
			TxnType:     apitypes.TransactionDel,
			StorageType: apitypes.StorageString,
			Key:         key,
		})
		w.Header().Add("Deleted", "true")
	}

	w.WriteHeader(http.StatusOK)
}

func (s *Service) mapPutHandler(w http.ResponseWriter, req *http.Request) {
	logOnEndpointHit(req.RequestURI, req.Method, req.RemoteAddr)

	key := mux.Vars(req)["key"]
	body, err := io.ReadAll(req.Body)
	defer req.Body.Close()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	hashMap := make(map[string]string)
	err = json.Unmarshal(body, &hashMap)
	if err != nil {
		log.Logger.Error("Failed to unmarshal the body in mapPutHandler")
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	cmd := s.storage[apitypes.StorageMap].Put(key, newCmdResult(hashMap))
	if cmd.err != nil {
		http.Error(w, cmd.err.Error(), http.StatusInternalServerError)
		return
	}

	s.sendTransaction(&apitypes.Transaction{
		TxnType:     apitypes.TransactionPut,
		StorageType: apitypes.StorageMap,
		Key:         key,
		Data:        hashMap,
	})

	w.WriteHeader(http.StatusOK)
}

func (s *Service) mapGetHandler(w http.ResponseWriter, req *http.Request) {
	logOnEndpointHit(req.RequestURI, req.Method, req.RemoteAddr)

	key := mux.Vars(req)["key"]
	cmd := s.storage[apitypes.StorageMap].Get(key, newCmdResult())
	if cmd.err != nil {
		http.Error(w, cmd.err.Error(), http.StatusNotFound)
		return
	}

	s.sendTransaction(&apitypes.Transaction{
		Timestamp:   time.Now(),
		TxnType:     apitypes.TransactionGet,
		StorageType: apitypes.StorageMap,
		Key:         key,
	})

	// NOTE: Maybe instead of transferring a stream of bytes, we could send the data
	// for the map in a json format? The content-type would have to be changed to application/json
	bytes, err := json.Marshal(cmd.result)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Add("Content-Type", "application/octet-stream")
	w.Header().Add("Content-Length", fmt.Sprintf("%d", len(bytes)))

	w.WriteHeader(http.StatusOK)
	w.Write(bytes)
}

func (s *Service) mapDeleteHandler(w http.ResponseWriter, req *http.Request) {
	logOnEndpointHit(req.RequestURI, req.Method, req.RemoteAddr)

	key := mux.Vars(req)["key"]
	cmd := s.storage[apitypes.StorageMap].Del(key, newCmdResult())
	if cmd.result.(bool) {
		s.sendTransaction(&apitypes.Transaction{
			Timestamp:   time.Now(),
			TxnType:     apitypes.TransactionDel,
			StorageType: apitypes.StorageMap,
			Key:         key,
		})
		w.Header().Add("Deleted", "true")
	}

	w.WriteHeader(http.StatusOK)
}

func (s *Service) intPutHandler(w http.ResponseWriter, req *http.Request) {
	logOnEndpointHit(req.RequestURI, req.Method, req.RemoteAddr)
	// TODO: Check the range the integer was passed,
	// because int on the python side is not equivalent to int in golang and other languages.
	key := mux.Vars(req)["key"]
	body, err := io.ReadAll(req.Body)
	defer req.Body.Close()
	if err != nil {
		log.Logger.Error("Failed to read the body inside intAddHandler, error %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	res, err := strconv.ParseInt(string(body), 10, 32)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	val := int32(res)
	cmd := s.storage[apitypes.StorageInt].Put(key, newCmdResult(val))
	if cmd.err != nil {
		http.Error(w, cmd.err.Error(), http.StatusInternalServerError)
		return
	}

	s.sendTransaction(&apitypes.Transaction{
		Timestamp:   time.Now(),
		TxnType:     apitypes.TransactionPut,
		StorageType: apitypes.StorageInt,
		Key:         key,
		Data:        val,
	})

	w.WriteHeader(http.StatusOK)
}

func (s *Service) intGetHandler(w http.ResponseWriter, req *http.Request) {
	logOnEndpointHit(req.RequestURI, req.Method, req.RemoteAddr)

	key := mux.Vars(req)["key"]
	cmd := s.storage[apitypes.StorageInt].Get(key, newCmdResult())
	if cmd.err != nil {
		http.Error(w, cmd.err.Error(), http.StatusNotFound)
		return
	}

	s.sendTransaction(&apitypes.Transaction{
		Timestamp:   time.Now(),
		TxnType:     apitypes.TransactionGet,
		StorageType: apitypes.StorageInt,
		Key:         key,
	})

	w.Header().Add("Conent-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(fmt.Sprintf("%d", cmd.result)))
}

func (s *Service) intDeleteHandler(w http.ResponseWriter, req *http.Request) {
	logOnEndpointHit(req.RequestURI, req.Method, req.RemoteAddr)

	key := mux.Vars(req)["key"]
	cmd := s.storage[apitypes.StorageInt].Del(key, newCmdResult())
	if cmd.result.(bool) {
		s.sendTransaction(&apitypes.Transaction{
			Timestamp:   time.Now(),
			TxnType:     apitypes.TransactionDel,
			StorageType: apitypes.StorageInt,
			Key:         key,
		})
		w.Header().Add("Deleted", "true")
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Service) intIncrHandler(w http.ResponseWriter, req *http.Request) {
	logOnEndpointHit(req.RequestURI, req.Method, req.RemoteAddr)

	key := mux.Vars(req)["key"]

	intStorage := s.storage[apitypes.StorageInt].(*IntStorage)
	cmd := intStorage.Incr(key, newCmdResult())
	if cmd.err != nil {
		http.Error(w, cmd.err.Error(), http.StatusInternalServerError)
		return
	}
	s.sendTransaction(&apitypes.Transaction{
		Timestamp:   time.Now(),
		TxnType:     apitypes.TransactionIncr,
		StorageType: apitypes.StorageInt,
		Key:         key,
	})

	// response body should contain the preivous value
	contents := strconv.FormatInt(int64(cmd.result.(int32)), 10)
	w.Header().Add("Content-Type", "application/octet-stream")
	w.Header().Add("Content-Length", fmt.Sprintf("%d", len(contents)))
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(contents))
}

func (s *Service) intIncrByHandler(w http.ResponseWriter, req *http.Request) {
	logOnEndpointHit(req.RequestURI, req.Method, req.RemoteAddr)

	key := mux.Vars(req)["key"]
	bytes, err := io.ReadAll(req.Body)
	defer req.Body.Close()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	res, err := strconv.ParseInt(string(bytes), 10, 32)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	val := int32(res)
	intStorage := s.storage[apitypes.StorageInt].(*IntStorage)
	cmd := intStorage.IncrBy(key, newCmdResult(val))
	if cmd.err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	s.sendTransaction(&apitypes.Transaction{
		Timestamp:   time.Now(),
		TxnType:     apitypes.TransactionIncrBy,
		StorageType: apitypes.StorageInt,
		Key:         key,
		Data:        val,
	})

	// response should contain the previously inserted value
	contents := strconv.FormatInt(int64(cmd.result.(int32)), 10)
	w.Header().Add("Content-Type", "application/octet-stream")
	w.Header().Add("Content-Length", fmt.Sprintf("%d", len(contents)))
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(contents))
}

func (s *Service) floatGetHandler(w http.ResponseWriter, req *http.Request) {
	logOnEndpointHit(req.RequestURI, req.Method, req.RemoteAddr)

	key := mux.Vars(req)["key"]
	cmd := s.storage[apitypes.StorageFloat].Get(key, newCmdResult())
	if cmd.err != nil {
		http.Error(w, cmd.err.Error(), http.StatusNotFound)
		return
	}

	s.sendTransaction(&apitypes.Transaction{
		Timestamp:   time.Now(),
		TxnType:     apitypes.TransactionGet,
		StorageType: apitypes.StorageFloat,
		Key:         key,
	})

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(fmt.Sprintf("%e", cmd.result)))
}

func (s *Service) floatPutHandler(w http.ResponseWriter, req *http.Request) {
	// If the value with the given key already exists,
	// and the value is the same, we shouldn't make any transactions,
	// because it would be a duplicate and only exhaust the memory.
	// We can introduce an update transaction for example as well.
	logOnEndpointHit(req.RequestURI, req.Method, req.RemoteAddr)

	key := mux.Vars(req)["key"]
	body, err := io.ReadAll(req.Body)
	defer req.Body.Close()
	if err != nil {
		err = fmt.Errorf("float storage: failed to read the request body %v", err)
		log.Logger.Error(err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	res, err := strconv.ParseFloat(string(body), 32)
	if err != nil {
		err = fmt.Errorf("float storage: failed to parse float %v", err)
		log.Logger.Error(err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	val := float32(res)
	s.storage[apitypes.StorageFloat].Put(key, newCmdResult(val))

	s.sendTransaction(&apitypes.Transaction{
		Timestamp:   time.Now(),
		TxnType:     apitypes.TransactionPut,
		StorageType: apitypes.StorageFloat,
		Key:         key,
		Data:        val,
	})

	w.WriteHeader(http.StatusOK)
}

func (s *Service) floatDeleteHandler(w http.ResponseWriter, req *http.Request) {
	logOnEndpointHit(req.RequestURI, req.Method, req.RemoteAddr)

	key := mux.Vars(req)["key"]

	cmd := s.storage[apitypes.StorageFloat].Del(key, newCmdResult())
	if cmd.result.(bool) {
		s.sendTransaction(&apitypes.Transaction{
			Timestamp:   time.Now(),
			TxnType:     apitypes.TransactionDel,
			StorageType: apitypes.StorageFloat,
			Key:         key,
		})
		w.Header().Add("Deleted", "true")
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Service) uintPutHandler(w http.ResponseWriter, req *http.Request) {
	logOnEndpointHit(req.RequestURI, req.Method, req.RemoteAddr)

	key := mux.Vars(req)["key"]

	// TODO: The value should be a part of a query string, so we don't need to read the body.
	body, err := io.ReadAll(req.Body)
	if err != nil {
		err := fmt.Errorf("uint storage: Failed to read the request body %v", err)
		log.Logger.Error(err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	res, err := strconv.ParseUint(string(body), 10, 32)
	if err != nil {
		err := fmt.Errorf("uint storage: Failed to parse the value %v", err)
		log.Logger.Error(err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	value := uint32(res)
	s.storage[apitypes.StorageUint].Put(key, newCmdResult(value))

	s.sendTransaction(&apitypes.Transaction{
		Timestamp:   time.Now(),
		TxnType:     apitypes.TransactionPut,
		StorageType: apitypes.StorageUint,
		Key:         key,
		Data:        value,
	})
}

func (s *Service) uintGetHandler(w http.ResponseWriter, req *http.Request) {
	logOnEndpointHit(req.RequestURI, req.Method, req.RemoteAddr)

	key := mux.Vars(req)["key"]

	cmd := s.storage[apitypes.StorageUint].Get(key, newCmdResult())
	if cmd.err != nil {
		err := fmt.Errorf("uint storage: %v", cmd.err)
		log.Logger.Error(err.Error())
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	s.sendTransaction(&apitypes.Transaction{
		Timestamp:   time.Now(),
		TxnType:     apitypes.TransactionGet,
		StorageType: apitypes.StorageUint,
		Key:         key,
	})

	w.Write([]byte(fmt.Sprintf("%d", cmd.result.(uint32))))
}

func (s *Service) uintDelHandler(w http.ResponseWriter, req *http.Request) {
	logOnEndpointHit(req.RequestURI, req.Method, req.RemoteAddr)

	key := mux.Vars(req)["key"]

	cmd := s.storage[apitypes.StorageUint].Del(key, newCmdResult())
	if cmd.result.(bool) {
		s.sendTransaction(&apitypes.Transaction{
			Timestamp:   time.Now(),
			TxnType:     apitypes.TransactionDel,
			StorageType: apitypes.StorageUint,
			Key:         key,
		})
		w.Header().Add("Deleter", "true")
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Service) delKeyHandler(w http.ResponseWriter, req *http.Request) {
	logOnEndpointHit(req.RequestURI, req.Method, req.RemoteAddr)

	key := mux.Vars(req)["key"]

	for _, storage := range s.storage {
		cmd := storage.Del(key, newCmdResult())
		if cmd.result.(bool) {
			s.sendTransaction(&apitypes.Transaction{
				Timestamp:   time.Now(),
				TxnType:     apitypes.TransactionDel,
				StorageType: cmd.storageType,
				Key:         key,
			})

			w.Header().Add("Deleted", "true")
			break
		}
	}
	w.WriteHeader(http.StatusOK)
}

func echo(param string) string {
	res := []rune(param)
	for i := 0; i < len(res); i++ {
		if unicode.IsLetter(res[i]) {
			if unicode.IsLower(res[i]) {
				res[i] = unicode.ToUpper(res[i])
				continue
			}
			res[i] = unicode.ToLower(res[i])
		}
	}
	return string(res)
}

func hello() string {
	return fmt.Sprintf("Hello from %s:%s service!", info.ServiceName(), info.ServiceVersion())
}

func fibo(n int) int {
	if n == 0 {
		return 0
	}
	if n == 1 || n == 2 {
		return 1
	}
	return fibo(n-1) + fibo(n-2)
}

func fiboHandler(w http.ResponseWriter, req *http.Request) {
	// Example URL: http://127.0.0.1:5000/kvs/v1-0-0/fibo?n=12
	logOnEndpointHit(req.RequestURI, req.Method, req.RemoteAddr)

	path, err := req.URL.Parse(req.RequestURI)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	query, err := url.ParseQuery(path.RawQuery)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if !path.Query().Has("n") {
		http.Error(w, "Invalid request syntax, {n} query parameter is not found", http.StatusBadRequest)
		return
	}

	n, _ := strconv.Atoi(query["n"][0])
	result := fibo(n)

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(strconv.Itoa(result)))
}

func echoHandler(w http.ResponseWriter, req *http.Request) {
	logOnEndpointHit(req.RequestURI, req.Method, req.RemoteAddr)

	buf, err := io.ReadAll(req.Body)
	defer req.Body.Close()

	if err != nil && err != io.EOF {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	val := echo(string(buf))

	w.Header().Add("Content-Type", "text/plain")
	w.Header().Add("Content-Length", fmt.Sprint(len(val)))
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(val))
}

func helloHandler(w http.ResponseWriter, req *http.Request) {
	logOnEndpointHit(req.RequestURI, req.Method, req.RemoteAddr)

	res := hello()

	w.Header().Add("Content-Type", "application/octet-stream")
	w.Header().Add("Content-Length", fmt.Sprint(len(res)))
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(res))
}

func logOnEndpointHit(reqURI, method, remoteAddr string) {
	log.Logger.Info("Endpoint %s, method %s, remoteAddr %s", reqURI, method, remoteAddr)
}

func (s *Service) BindRPCHandler(method, funcName string, callback HandlerCallback) {
	if s.running {
		log.Logger.Error("Failed to bind {%s} RPC, service is already running", funcName)
		return
	}

	if callback == nil {
		log.Logger.Error("Failed to bind {%s} RPC, handler cannot be nil", funcName)
		return
	}

	s.rpcHandlers = append(
		s.rpcHandlers,
		&RPCHandler{method: method, funcName: funcName, cb: callback},
	)
}

// func (s *Service) processSavedTransactions() error {
// 	var err error
// 	var event Event

// 	eventsChan, errorsChan := s.txnLogger.ReadEvents()

// 	for {
// 		select {
// 		case event = <-eventsChan:
// 			switch event.txnType {
// 			case txnPut:
// 				cmd := s.storage[event.storageType].Put(event.key, newCmdResult(event.value))
// 				err = cmd.err

// 			case txnGet:
// 				cmd := s.storage[event.storageType].Get(event.key, newCmdResult())
// 				err = cmd.err

// 			case txnDel:
// 				cmd := s.storage[event.storageType].Del(event.key, newCmdResult())
// 				err = cmd.err

// 			case txnIncr:
// 				intStorage := s.storage[event.storageType].(*IntStorage)
// 				cmd := intStorage.IncrBy(event.key, newCmdResult())
// 				err = cmd.err

// 			case txnIncrBy:
// 				intStorage := s.storage[event.storageType].(*IntStorage)
// 				cmd := intStorage.Incr(event.key, newCmdResult(event.value))
// 				err = cmd.err
// 			}

// 		case err = <-errorsChan:
// 			// Error received while reading events
// 			if err != io.EOF && err != nil {
// 				return err
// 			}
// 			return nil
// 		}

// 		// Error encountered while inserting events into the storage
// 		if err != nil {
// 			return err
// 		}

// 		log.Logger.Info("Saved event: Event{id: %d, t: %s, key: %s, value: %v, timestamp: %s}",
// 			event.id, event.txnType, event.key, event.value, event.timestamp.Format(time.DateTime))

// 		event = Event{}
// 	}
// }

// func (s *Service) writeTransaction(txnType TxnType, storage StorageType, key string, value interface{}) {
// 	s.transactionChan <- &api.Transaction{}
// 	// if !s.settings.TxnDisabled {
// 	// 	s.txnLogger.WriteTransaction(txnType, storage, key, value)
// 	// }
// }

func (s *Service) sendTransaction(transact *apitypes.Transaction) {
	s.transactChan <- transact
}

func NewService(settings *ServiceSettings, txnClient api.TransactionServiceClient) *Service {
	// https://stackoverflow.com/questions/39320025/how-to-stop-http-listenandserve
	service := &Service{
		Server: &http.Server{
			Addr: settings.Endpoint,
		},
		settings:    settings,
		rpcHandlers: make([]*RPCHandler, 0),
		storage:     make(map[apitypes.TransactionStorageType]Storage),
		txnLogger:   settings.TxnLogger,

		////////////////////////////////set transaction service client////////////////////////////////
		txnClient:    txnClient,
		transactChan: make(chan *apitypes.Transaction),
	}

	service.storage[apitypes.StorageInt] = newIntStorage()
	service.storage[apitypes.StorageUint] = newUintStorage()
	service.storage[apitypes.StorageFloat] = newFloatStorage()
	service.storage[apitypes.StorageString] = newStrStorage()
	service.storage[apitypes.StorageMap] = newMapStorage()

	// NOTE: This has to be executed after both transaction logger AND the storage is initialized
	// if err := service.processSavedTransactions(); err != nil {
	// 	log.Logger.Fatal("Failed to fetch saved transactions %v", err)
	// }

	service.BindRPCHandler("POST", "echo", echoHandler)
	service.BindRPCHandler("POST", "hello", helloHandler)
	service.BindRPCHandler("POST", "fibo", fiboHandler)

	return service
}

func (s *Service) Close() {
	if s.running {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := s.Server.Shutdown(ctx); err != nil {
			if err != context.DeadlineExceeded {
				log.Logger.Fatal("Failed to shut down a server %v", err)
			}
		}
	}
}

func (s *Service) Run() error {
	defer s.txnLogger.Close()

	// NOTE: We cannot run the server, until we read all the transactions from transaction serivce,
	// so, if there were any in the database OR a file, we put them into memory storage.

	//////////////////////////////////open a stream for reading transactions//////////////////////////////////
	readTransactionStream, err := s.txnClient.ReadTransactions(context.Background(), &emptypb.Empty{})
	if err != nil {
		log.Logger.Error("Failed to open read transaction stream %v", err)
		return fmt.Errorf("failed to open a stream for reading transactions %v", err)
	}

	// NOTE: This all will be moved to an api client, so we don't interact with GRPC types from the service.

	for {
		transact, err := readTransactionStream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Logger.Error("Failed to read transaction %v", err)
			return fmt.Errorf("failed to read transaction %v", err)
		}

		txnType := apitypes.TransactionType(apitypes.TransactionTypeValue[transact.TxnType.String()])
		txnStorageType := apitypes.TransactionStorageType(apitypes.StorageTypeValue[transact.StorageType.String()])

		log.Logger.Info("Received transaction %s", transact.TxnType.String())

		switch txnType {
		case apitypes.TransactionPut:
			var value interface{}
			switch txnStorageType {
			case apitypes.StorageInt:
				value = transact.Data.GetIntValue()

			case apitypes.StorageUint:
				value = transact.Data.GetUintValue()

			case apitypes.StorageFloat:
				value = transact.Data.GetFloatValue()

			case apitypes.StorageString:
				value = transact.Data.GetStrValue()

			case apitypes.StorageMap:
				value = transact.Data.GetMapValue().GetData()
			}

			log.Logger.Info("Value %v", value)

			if cmd := s.storage[txnStorageType].Put(transact.Key, newCmdResult(value)); cmd.err != nil {
				log.Logger.Error("Failed to store %s transaction %v",
					apitypes.TransactionTypeName[int32(apitypes.TransactionDel)], cmd.err)
				return cmd.err
			}

		case apitypes.TransactionGet:
			if cmd := s.storage[txnStorageType].Get(transact.Key, newCmdResult()); cmd.err != nil {
				log.Logger.Error("Failed to store %s transaction %v",
					apitypes.TransactionTypeName[int32(apitypes.TransactionDel)], cmd.err)
				return cmd.err
			}

		case apitypes.TransactionDel:
			if cmd := s.storage[txnStorageType].Del(transact.Key, newCmdResult()); cmd.err != nil {
				log.Logger.Error("Failed to store %s transaction %v",
					apitypes.TransactionTypeName[int32(apitypes.TransactionDel)], cmd.err)
				return cmd.err
			}

		case apitypes.TransactionIncr:

		case apitypes.TransactionIncrBy:
			// NOTE: We would have to retrieve a value here as well.
		}
	}

	/////////////////////////////////////Open a stream for writing transactions/////////////////////////////////////
	writeTransactionStream, err := s.txnClient.WriteTransactions(context.Background())
	if err != nil {
		log.Logger.Error("Failed to open write transactions stream %v", err)
		return fmt.Errorf("failed to open a stream for writing transactions %v", err)
	}

	go func() {
		for {
			select {
			case transact := <-s.transactChan:
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
					transactionData = &api.TransactionData{Kind: &api.TransactionData_NullValue{}}
				}

				protoTransaction := &api.Transaction{
					TxnType:     api.TxnType(transact.TxnType),
					StorageType: api.StorageType(transact.StorageType),
					Timestamp:   timestamppb.New(transact.Timestamp),
					Key:         transact.Key,
					Data:        transactionData,
				}

				log.Logger.Info("Sending %s transaction", apitypes.TransactionTypeName[int32(transact.TxnType)])

				err := writeTransactionStream.Send(protoTransaction)
				if err != nil {
					log.Logger.Error("Failed to send transaction %v", err)
					// What do we do here? Shut down the service?
				}
			}
		}
	}()

	////////////////////////////////////////service logic////////////////////////////////////////
	s.running = true

	shutdownCtx, cancel := context.WithCancel(context.Background())
	defer func() {
		cancel()
		s.txnLogger.WaitForPendingTransactions()
	}()

	// This goroutine won't be leaked
	go s.txnLogger.ProcessTransactions(shutdownCtx)

	router := mux.NewRouter().StrictSlash(true)
	// router.Path()
	subrouter := router.PathPrefix(fmt.Sprintf("/%s/%s/", info.ServiceName(), info.ServiceVersion())).Subrouter()

	// Bind all rpc handlers
	for _, hd := range s.rpcHandlers {
		subrouter.Path("/" + hd.funcName).HandlerFunc(hd.cb).Methods(hd.method)
	}

	// TODO: Rewrite endpoints with /map/put/key/..., /map/get/key/...
	subrouter.Path("/map-put/{key:[0-9A-Za-z_]+}").HandlerFunc(s.mapPutHandler).Methods("PUT")
	subrouter.Path("/map-get/{key:[0-9A-Za-z_]+}").HandlerFunc(s.mapGetHandler).Methods("GET")
	subrouter.Path("/map-del/{key:[0-9A-Za-z_]+}").HandlerFunc(s.mapDeleteHandler).Methods("DELETE")

	subrouter.Path("/str-put/{key:[0-9A-Za-z_]+}").HandlerFunc(s.stringPutHandler).Methods("PUT")
	subrouter.Path("/str-get/{key:[0-9A-Za-z_]+}").HandlerFunc(s.stringGetHandler).Methods("GET")
	subrouter.Path("/str-del/{key:[0-9A-Za-z_]+}").HandlerFunc(s.stringDeleteHandler).Methods("DELETE")

	subrouter.Path("/int-put/{key:[0-9A-Za-z_]+}").HandlerFunc(s.intPutHandler).Methods("PUT")
	subrouter.Path("/int-get/{key:[0-9A-Za-z_]+}").HandlerFunc(s.intGetHandler).Methods("GET")
	subrouter.Path("/int-del/{key:[0-9A-Za-z_]+}").HandlerFunc(s.intDeleteHandler).Methods("DELETE")
	subrouter.Path("/int-incr/{key:[0-9A-Za-z_]+}").HandlerFunc(s.intIncrHandler).Methods("PUT")
	subrouter.Path("/int-incrby/{key:[0-9A-Za-z_]+}").HandlerFunc(s.intIncrByHandler).Methods("PUT")

	subrouter.Path("/float-put/{key:[0-9A-Za-z_]+}").HandlerFunc(s.floatPutHandler).Methods("PUT")
	subrouter.Path("/float-get/{key:[0-9A-Za-z_]+}").HandlerFunc(s.floatGetHandler).Methods("GET")
	subrouter.Path("/float-del/{key:[0-9A-Za-z_]+}").HandlerFunc(s.floatDeleteHandler).Methods("DELETE")

	// NOTE: Can we remove put/add/del and only rely on HTTP method?
	subrouter.Path("/uint/put/{key:[0-9A-Za-z_]+}").HandlerFunc(s.uintPutHandler).Methods("PUT")
	subrouter.Path("/uint/get/{key:[0-9A-Za-z_]+}").HandlerFunc(s.uintGetHandler).Methods("GET")
	subrouter.Path("/uint/del/{key:[0-9A-Za-z_]+}").HandlerFunc(s.uintDelHandler).Methods("DELETE")

	// Endpoint to delete a key from any type of storage
	subrouter.Path("/del/{key:[0-9A-Za-z_]+}").HandlerFunc(s.delKeyHandler).Methods("DELETE")

	s.Server.Handler = router

	log.Logger.Info("%s:%s service is running %s", info.ServiceName(), info.ServiceVersion(), s.settings.Endpoint)
	if err := s.Server.ListenAndServe(); err != http.ErrServerClosed {
		log.Logger.Fatal("Server terminated abnormally %v", err)
	}

	return nil
}
