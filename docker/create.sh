docker run --name cas1 -p 9042:9042 -e CASSANDRA_CLUSTER_NAME=BigTrains -d cassandra
docker run --name cas2 -e CASSANDRA_SEEDS="$(docker inspect --format='{{.NetworkSettings.IPAddress}}' cas1)" -e CASSANDRA_CLUSTER_NAME=BigTrains -d cassandra
# docker run --name cas3 -e CASSANDRA_SEEDS="$(docker inspect --format='{{.NetworkSettings.IPAddress}}' cas1)" -e CASSANDRA_CLUSTER_NAME=BigTrains -d cassandra
docker exec -ti cas1 nodetool -Dcom.sun.jndi.rmiURLParsing=legacy status