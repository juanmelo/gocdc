package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"log"
)

func (cdc *ChangeDataCapture) MustNewConsumer() sarama.Consumer {
	config := sarama.NewConfig()
	consumer, err := sarama.NewConsumer(cdc.brokers, config)
	if err != nil {
		fmt.Println("Could not create consumer: ", err)
		panic(err)
	}
	return consumer
}

func SendMessage(brokers []string, topic string, message string) {
	producer, err := sarama.NewSyncProducer(brokers, nil)
	if err != nil {
		fmt.Println("Could not create producer: ", err)
		panic(err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	msg := &sarama.ProducerMessage{Topic: topic, Value: sarama.StringEncoder(message)}
	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		log.Printf("FAILED to send message: %s\n", err)
	} else {
		log.Printf("> message sent to partition %d at offset %d\n", partition, offset)
	}
}

func (cdc *ChangeDataCapture) subscribe(topic string, consumer sarama.Consumer) {
	partitionList, err := consumer.Partitions(topic) //get all partitions on the given topic
	log.Print(partitionList)
	if err != nil {
		fmt.Println("Error retrieving partitionList ", err)
	}
	initialOffset := sarama.OffsetNewest //OffsetOldest OffsetNewest //get offset for the oldest message on the topic

	for _, partition := range partitionList {
		pc, _ := consumer.ConsumePartition(topic, partition, initialOffset)

		go func(pc sarama.PartitionConsumer) {
			for message := range pc.Messages() {
				cdc.messageReceived(message)
			}
		}(pc)
	}
}

func (cdc *ChangeDataCapture) messageReceived(message *sarama.ConsumerMessage) {
	kafkaLog, err := toKafkaLog(message.Value, cdc.pkFormat)
	if err == nil {
		log.Printf("%v : %v", kafkaLog.Operation, kafkaLog.TableName)
		cdc.insert(kafkaLog)
	}
}
