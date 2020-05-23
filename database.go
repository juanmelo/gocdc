package main

import (
	"encoding/xml"
	"fmt"
	"strconv"
	"strings"
	"time"
)

type KafkaLog struct {
	Id         int    `db:"go_kafka_log_id"`
	TxnId      int    `db:"txn_id"`
	MsgId      int    `db:"msg_id"`
	SchemaName string `db:"schema_name"`
	TableName  string `db:"table_name"`
	Operation  string
	PKValue    int `db:"pk_value"`
	Payload    string
	CommitTime time.Time `db:"commit_time"`
}

func toKafkaLog(messageValue []byte, pkFormat string) (KafkaLog, error) {
	var openTarget OpenTarget
	err := xml.Unmarshal(messageValue, &openTarget)
	if err == nil {
		if openTarget.Table.Name != "" {
			tableSchema := strings.Split(openTarget.Table.Name, ".")

			var kafkaLog = KafkaLog{
				TxnId:      openTarget.Txn.Id,
				MsgId:      openTarget.Txn.MsgIdx,
				SchemaName: tableSchema[0],
				TableName:  tableSchema[1],
				Operation:  openTarget.Table.Command.Operation,
				PKValue:    findPKValue(openTarget, tableSchema[1], pkFormat),
				Payload:    string(messageValue),
				CommitTime: openTarget.Txn.CommitTime.Time}

			return kafkaLog, nil
		}
	}
	return KafkaLog{}, err
}

func findPKValue(openTarget OpenTarget, tableName string, pkFormat string) int {
	pkColumn := strings.ReplaceAll(pkFormat, "{tableName}", tableName)
	for _, column := range openTarget.Table.Command.Row.Lookup.Columns {
		if strings.EqualFold(column.Name, pkColumn) {
			pkValue, _ := strconv.Atoi(column.Value)
			return pkValue
		}
	}
	return 0
}

func (cdc *ChangeDataCapture) insert(kafkaLog KafkaLog) {
	query := "insert into go_kafka_log (txn_id, msg_id, schema_name, table_name, operation, pk_value, payload,commit_time,created_date) " +
		"values(:txn_id, :msg_id, :schema_name, :table_name, :operation, :pk_value, :payload, :commit_time,sysdate())"
	_, error := cdc.db.NamedExec(query, &kafkaLog)
	if error != nil {
		fmt.Println("Error persisting ", error)
	}
}
