#1/bin/bash

COMPOSE_FILES="-f mongodb.yaml -f minio.yaml -f kafka_cluster.yaml -f spark.yaml -f superset.yaml"

echo "Starting Docker Compose with the following files: $COMPOSE_FILES"

docker-compose $COMPOSE_FILES up -d

# echo "Checking container status..."
# docker-compose ps

# Lệnh để dừng Docker Compose (nếu cần chạy riêng)
# docker-compose $COMPOSE_FILES down

# Lệnh để thực thi: ./run.sh