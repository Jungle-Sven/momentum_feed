
1 build

sudo docker build -t momentum_feed .

2 run

docker run -d --restart always --name momentum_feed_container --network momentum_network --log-opt mode=non-blocking --log-opt max-buffer-size=10m momentum_feed

3 stop

sudo docker stop momentum_feed_container

4 remove

sudo docker rm momentum_feed_container

5 logs

sudo docker logs momentum_feed_container
