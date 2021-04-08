#!/bin/bash

cd ./scripts/
# python3 ./generator_logs.py --start-date 01.01.2020 --end-date 25.05.2021
docker rm -f hadoop
docker rmi custom_centos:latest
docker build --rm -t custom_centos - < Dockerfile
docker run --privileged -d -p 50070:50070 -ti -e container=docker --name=hadoop -v /sys/fs/cgroup:/sys/fs/cgroup  custom_centos /usr/sbin/init
docker cp ./logs hadoop:/project/
docker cp ./run_hadoop.sh hadoop:/project/
cd ..
docker cp ./target/lab1-1.0-SNAPSHOT-jar-with-dependencies.jar hadoop:/project/
docker exec -it hadoop /bin/bash
