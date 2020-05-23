package main

import (
	"io/ioutil"
	"log"
	"testing"
)

func TestDecodePayload(t *testing.T) {
	data, err := ioutil.ReadFile("sample/shareplex.xml")
	if err != nil {
		t.Error("Error loading file")
	}

	kafkaLog, err := toKafkaLog(data, "{tableName}_id")
	if err != nil {
		t.Error("Error parsing to KafkaLog")
	}
	log.Printf("%v", kafkaLog)
	//SendMessage([]string{"localhost:9092"},"shareplex", string(data))
}
