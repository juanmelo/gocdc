CREATE TABLE `go_kafka_log` (
  `go_kafka_log_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `txn_id` bigint(20) DEFAULT NULL,
  `msg_id` bigint(20) DEFAULT NULL,
  `schema_name` varchar(256) DEFAULT NULL,
  `table_name` varchar(256) DEFAULT NULL,
  `operation` varchar(16) DEFAULT NULL,
  `pk_value` bigint(20) DEFAULT NULL,
  `payload` text,
  `commit_time` timestamp NULL DEFAULT NULL,
  `created_date` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`go_kafka_log_id`)
)