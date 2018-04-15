package main

import (
	"github.com/Shopify/sarama"
	"github.com/jmoiron/sqlx"
	_ "github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"
	//"database/sql"
	"log"
	"fmt"
	"encoding/xml"
	"strings"
	"os"
	"net/http"
	"time"
)

type OpenTarget struct {
	XMLName xml.Name `xml:"opentarget"`
	Txn Txn `xml:"txn"`
	Table Table `xml:"tbl"`
}
type Txn struct {
	XMLName xml.Name `xml:"txn"`
	Id int `xml:"id,attr"`
	MsgIdx int `xml:"msgIdx,attr"`
	CommitTime customTime `xml:"commitTime,attr"`
}
type Table struct{
	XMLName xml.Name `xml:"tbl"`
	Name string `xml:"name,attr"`
	Command Command `xml:"cmd"`
}

type Command struct{
	XMLName xml.Name `xml:"cmd"`
	Operation string `xml:"ops,attr"`
	Row Row `xml:"row"`
}
type Row struct{
	XMLName xml.Name `xml:"row"`
	Id string `xml:"id,attr"`
	Columns []Column `xml:"col"`
	Lookup Lookup `xml:"lkup"`
}
type Column struct{
	XMLName xml.Name `xml:"col"`
	Name string `xml:"name,attr"`
	Value string `xml:",chardata"`
}

type Lookup struct{
	XMLName xml.Name `xml:"lkup"`
	Columns []Column `xml:"col"`
}


type ChangeDataCapture struct {
	db *sqlx.DB
	brokers []string
}

type KafkaLog struct {
	Id int `db:"go_kafka_log_id"`
	TxnId int `db:"txn_id"`
	MsgId int `db:"msg_id"`
	SchemaName string `db:"schema_name"`
	TableName string `db:"table_name"`
	Operation string
	Payload string
	CommitTime time.Time `db:"commit_time"`
}

type customTime struct {
	time.Time
}

func (c *customTime) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	var v string
	d.DecodeElement(&v, &start)
	parse, _ := time.Parse("2006-01-02T15:04:05", v)
	*c = customTime{parse}
	return nil
}

func (c *customTime) UnmarshalXMLAttr(attr xml.Attr) error {
	parse, _ := time.Parse("2006-01-02T15:04:05", attr.Value)
	*c = customTime{parse}
	return nil
}

//var brokers = []string{"cl-rdv-kafka1.qualifacts.com:6667","cl-rdv-kafka2.qualifacts.com:6667","cl-rdv-kafka3.qualifacts.com:6667"}
//var topic = "team3c"

func main(){
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}
	var brokers = strings.Split(os.Getenv("BROKERS"), ",")
	var topic = os.Getenv("TOPIC")
	var databaseUrl = os.Getenv("DATABASE_URL")
	var httpAddress = os.Getenv("HTTP_ADDRESS")


	log.Printf("Reading topic %v from %v\n database: %v  httpAddress [%v] \n", topic, brokers, databaseUrl, httpAddress)

	db := sqlx.MustConnect("mysql", databaseUrl)
	db.Ping()

	defer db.Close()

	cdc := ChangeDataCapture{db: db, brokers: brokers}

	consumer := cdc.MustNewConsumer()
	cdc.subscribe(topic, consumer)

	defer func() {
		if err := consumer.Close(); err != nil {
			panic(err)
		}
	}()

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, "Hello Sarama!") })

	log.Fatal(http.ListenAndServe(httpAddress, nil))

}

func (cdc *ChangeDataCapture) MustNewConsumer() (sarama.Consumer) {
	config := sarama.NewConfig()
	consumer, err := sarama.NewConsumer(cdc.brokers, config)
	if err != nil {
		fmt.Println("Could not create consumer: ", err)
		panic(err)
	}
	return consumer
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
	cdc.processMessage(message.Value)
}

func (cdc *ChangeDataCapture) processMessage(messageValue []byte){
	var openTarget OpenTarget
	xml.Unmarshal(messageValue, &openTarget)

	if openTarget.Table.Name != "" {
		log.Printf("%v -> %v",openTarget.Table.Command.Operation, openTarget.Table.Name)
		tasch := strings.Split(openTarget.Table.Name, ".")

		var kafkaLog = KafkaLog{
			TxnId:openTarget.Txn.Id,
			MsgId:openTarget.Txn.MsgIdx,
			SchemaName:tasch[0],
			TableName:tasch[1],
			Operation:openTarget.Table.Command.Operation,
			Payload:string(messageValue),
			CommitTime:openTarget.Txn.CommitTime.Time}
		query:= "insert into go_kafka_log (txn_id, msg_id, schema_name, table_name, operation, payload,commit_time,created_date) " +
			"values(:txn_id, :msg_id, :schema_name, :table_name, :operation, :payload, :commit_time,sysdate())"
		_, error := cdc.db.NamedExec(query, &kafkaLog)
		if error !=nil{
			fmt.Println("Error persisting ", error)
		}
		//log.Printf("%v %v",kafkaLog, query)
	}

}
