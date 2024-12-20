# run from root directory - must have access to docker-compose.yml
protoc --go_out=. --go-grpc_out=. proto/cassandra.proto
docker-compose -f docker/docker-compose.yml up --build