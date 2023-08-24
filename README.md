A microservice recieves executed trades and orderbook updates from exchanges. Uses cryptofeed library. Saves data to local redis data storage.

1 build

sudo docker build -t momentum_feed .

2 run

docker run -d --restart always --name momentum_feed_container --network momentum_network --log-opt mode=non-blocking --log-opt max-size=10m --log-opt max-file=3 momentum_feed

3 stop

sudo docker stop momentum_feed_container

4 remove

sudo docker rm momentum_feed_container

5 logs

sudo docker logs momentum_feed_container
