docker kill streaming-producer
docker rm streaming-producer
docker rmi big-data_streaming-producer:latest
docker-compose -f ../docker-compose-2.yaml up -d