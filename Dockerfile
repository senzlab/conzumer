FROM golang:1.9

MAINTAINER Eranga Bandara (erangaeb@gmail.com)

# install dependencies
RUN go get github.com/Shopify/sarama
RUN go get github.com/wvanbergen/kafka/consumergroup

# env
ENV ZOOKEEPER_HOST dev.localhost
ENV ZOOKEEPER_PORT 2181
ENV TOPIC senz

# copy app
ADD . /app
WORKDIR /app

# build
RUN go build -o build/conzumer src/*.go

ENTRYPOINT ["/app/docker-entrypoint.sh"]
