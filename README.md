## Overview
KVS is a distributed key-value storage service written in Golang. It provides a functionality for storing pod data types in-memory of a separate process, potentially running on a remote machine. Currently it is capable of handling 32-bit signed/unsigned integers, 32-bit floats, strings, sets, and hash maps of type `map[string]string, or dict[str, str]`, but that would be extended in the future releases with an ebility to store custom structs and maps of any type. The service utilises REST API for communication with its clients, either Python or Golang, and [gRPC](https://grpc.io/) framework for interacting with another internal service for handling transactions. The transaction service (TXN) is responsbile for persisting all the requests, made to key-value storage, in a PostgreSQL database or in a binary file. That way, the state of the storage can be recreated by reading all the transactions from the database, in case the service crashes abnormally. TXN service is run in a separate docker container. 

## Running KVS service
Running key-value storage service is very straightforward and only requires [Docker](https://www.docker.com/products/docker-desktop/) to be installed on your machine. Once that is done, run `docker compose up` inside a root directory. That will spin up two docker containers, the first one running `PostgreSQL` database, and the second one running our key-value storage application. Everything is configured inside a docker [compose.yaml](https://github.com/isnastish/kvs/blob/master/compose.yaml) file.
If you see these logs, as a result of executing the command above, you have done everything correctly and your kvs service is up and running, together with PostgreSQL database for handling transactions.
![image](https://github.com/user-attachments/assets/bff5b9b4-652f-4faf-9391-f759aa63cf3c)

## Testing the api
Once your service is running, the simplest way of testing the storage would be to clone a KVS Python client repository and execute any cli command, `python -m kvs.cli echo "Hello From the Client"`, for instance. More information can be found here [using python client to test kvs service](https://github.com/isnastish/kvs-python-client), or you can run any test inside the Go client package with `go test`.



> **NOTE:** [feature/transaction-service](https://github.com/isnastish/kvs/tree/feature/transaction-service) branch contains the integration for TXN service using `gRPC` framework and is currently in development.
