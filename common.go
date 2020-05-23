package main

import (
	"encoding/xml"
	"github.com/jmoiron/sqlx"
	"time"
)

type OpenTarget struct {
	XMLName xml.Name `xml:"opentarget"`
	Txn     Txn      `xml:"txn"`
	Table   Table    `xml:"tbl"`
}
type Txn struct {
	XMLName    xml.Name   `xml:"txn"`
	Id         int        `xml:"id,attr"`
	MsgIdx     int        `xml:"msgIdx,attr"`
	CommitTime customTime `xml:"commitTime,attr"`
}
type Table struct {
	XMLName xml.Name `xml:"tbl"`
	Name    string   `xml:"name,attr"`
	Command Command  `xml:"cmd"`
}

type Command struct {
	XMLName   xml.Name `xml:"cmd"`
	Operation string   `xml:"ops,attr"`
	Row       Row      `xml:"row"`
}
type Row struct {
	XMLName xml.Name `xml:"row"`
	Id      string   `xml:"id,attr"`
	Columns []Column `xml:"col"`
	Lookup  Lookup   `xml:"lkup"`
}
type Column struct {
	XMLName xml.Name `xml:"col"`
	Name    string   `xml:"name,attr"`
	Value   string   `xml:",chardata"`
}

type Lookup struct {
	XMLName xml.Name `xml:"lkup"`
	Columns []Column `xml:"col"`
}

type ChangeDataCapture struct {
	db       *sqlx.DB
	brokers  []string
	pkFormat string
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
