COMPOSE_FILES="-f mongodb.yaml -f minio.yaml -f kafka_cluster.yaml -f spark.yaml -f superset.yaml"

echo "Stopping Docker Compose with the following files: $COMPOSE_FILES"

docker-compose $COMPOSE_FILES down