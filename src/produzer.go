package main

import (
    "fmt"
	"log"
    "os"
    "time"
	"github.com/Shopify/sarama"
	"github.com/wvanbergen/kafka/consumergroup"
)

type Consumer struct {
    ConsumerGroupName   string
	ZookeeperAddress    string
	Topic               string
}

const (
    zookeeperConn = "10.4.1.29:2181"
    cgroup = "zgroup"
    topic = "senz"
)

func main() {
    // setup sarama log to stdout
	sarama.Logger = log.New(os.Stdout, "", log.Ltime)

    // init consumer
    c, err := initConsumer()
    if err != nil {
        fmt.Println("Error consumer: ", err.Error())
        os.Exit(1)
    }

    // init consumer group
    cg, err := initConsumerGroup(c)
    if err != nil {
        fmt.Println("Error consumer goup: ", err.Error())
        os.Exit(1)
    }
    defer cg.Close()

    // run consumer
    consume(c, cg)
}

func initConsumer() (*Consumer, error) {
    // new consumer
    c := &Consumer {
        ConsumerGroupName: cgroup,
        ZookeeperAddress: zookeeperConn,
        Topic: topic,
    }

    return c, nil
}

func initConsumerGroup(c *Consumer)(*consumergroup.ConsumerGroup, error) {
    // consumer config
	config := consumergroup.NewConfig()
	config.Offsets.Initial = sarama.OffsetOldest
	config.Offsets.ProcessingTimeout = 10 * time.Second

    // join to consumer group
    cg, err := consumergroup.JoinConsumerGroup(c.ConsumerGroupName, []string{c.Topic}, []string{c.ZookeeperAddress}, config)
    if err != nil {
        return nil, err
    }

    return cg, err
}

func consume(c *Consumer, cg *consumergroup.ConsumerGroup) {
    for {
        select {
        case msg := <- cg.Messages():
			if msg.Topic != topic {
                continue
            }

            fmt.Println("Topic: ", msg.Topic)
            fmt.Println("Value: ", string(msg.Value))

            // commit to zookeeper
            err := cg.CommitUpto(msg)
            if err != nil {
                fmt.Println("Error commit zookeeper: ", err.Error())
            }
        }
    }
}

