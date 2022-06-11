docker run --name cas1 -p 9042:9042 -e HEAP_NEWSIZE=1M -e MAX_HEAP_SIZE=1024M -e CASSANDRA_CLUSTER_NAME=BigTrains -d cassandra:latest
docker run --name cas2 -e CASSANDRA_SEEDS="$(docker inspect --format='{{.NetworkSettings.IPAddress}}' cas1)" -e HEAP_NEWSIZE=1M -e MAX_HEAP_SIZE=1024M -e CASSANDRA_CLUSTER_NAME=BigTrains -d cassandra:latest
docker run --name cas3 -e CASSANDRA_SEEDS="$(docker inspect --format='{{.NetworkSettings.IPAddress}}' cas1)" -e HEAP_NEWSIZE=1M -e MAX_HEAP_SIZE=1024M -e CASSANDRA_CLUSTER_NAME=BigTrains -d cassandra:latest
docker run --name cas4 -e CASSANDRA_SEEDS="$(docker inspect --format='{{.NetworkSettings.IPAddress}}' cas1)" -e HEAP_NEWSIZE=1M -e MAX_HEAP_SIZE=1024M -e CASSANDRA_CLUSTER_NAME=BigTrains -d cassandra:latest
docker run --name cas5 -e CASSANDRA_SEEDS="$(docker inspect --format='{{.NetworkSettings.IPAddress}}' cas1)" -e HEAP_NEWSIZE=1M -e MAX_HEAP_SIZE=1024M -e CASSANDRA_CLUSTER_NAME=BigTrains -d cassandra:latest

# docker exec -ti cas1 nodetool -Dcom.sun.jndi.rmiURLParsing=legacy status

docker inspect --format='{{.NetworkSettings.IPAddress}}' cas1