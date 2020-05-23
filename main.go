package main

import (
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/joho/godotenv"
	"log"
	"net/http"
	"os"
	"strings"
)

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}
	var brokers = strings.Split(os.Getenv("BROKERS"), ",")
	var topic = os.Getenv("TOPIC")
	var databaseUrl = os.Getenv("DATABASE_URL")
	var httpAddress = os.Getenv("HTTP_ADDRESS")
	var pkFormat = os.Getenv("PRIMARY_KEY_FORMAT")

	log.Printf("Reading topic %v from %v\n database: %v  httpAddress [%v] \n", topic, brokers, databaseUrl, httpAddress)

	db := sqlx.MustConnect("mysql", databaseUrl)
	err = db.Ping()
	if err != nil {
		panic(err)
	}

	defer db.Close()

	cdc := ChangeDataCapture{db: db, brokers: brokers, pkFormat: pkFormat}

	consumer := cdc.MustNewConsumer()
	cdc.subscribe(topic, consumer)

	defer func() {
		if err := consumer.Close(); err != nil {
			panic(err)
		}
	}()

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "Hello Sarama!")
	})

	log.Fatal(http.ListenAndServe(httpAddress, nil))

}
