docker kill submit
docker rm submit
docker rmi big-data_submit:latest
docker-compose -f ../docker-compose-1.yaml up -d
