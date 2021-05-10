docker kill ml-batch
docker rm ml-batch
docker rmi big-data_ml-batch:latest
docker-compose -f ../docker-compose-3.yaml up -d
