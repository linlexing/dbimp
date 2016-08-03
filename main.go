package main

import (
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"

	log "github.com/Sirupsen/logrus"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-oci8"

	"flag"
)

func init() {
	os.Setenv("NLS_LANG", "AMERICAN_AMERICA.AL32UTF8")

}
func main() {
	driver := flag.String("driver", "", "db driver")
	dburl := flag.String("dburl", "", "database url")
	table := flag.String("table", "", "table name")
	truncate := flag.Bool("trunc", false, "trunc table before import")
	filename := flag.String("file", "export.dat", "select query sql,the table must empty")
	flag.Parse()
	if len(*table) == 0 {
		log.Panic("table is empty")
	}
	var strSql string
	file, err := os.Open(*filename)
	if err != nil {
		log.WithFields(log.Fields{
			"file": filename,
		}).Panic(err)
	}
	defer file.Close()
	cols := []string{}
	dec := gob.NewDecoder(file)
	if err = dec.Decode(&cols); err != nil {
		log.Panic(err)
	}
	switch *driver {
	case "oci8":
		pnames := []string{}
		for i, _ := range cols {
			pnames = append(pnames, fmt.Sprintf(":p%d", i))
		}
		strSql = fmt.Sprintf("insert into %s(%s)values(%s)",
			*table, strings.Join(cols, ",\n"), strings.Join(pnames, ",\n"))

	case "mysql":
		pnames := []string{}
		for _, _ = range cols {
			pnames = append(pnames, "?")
		}
		strSql = fmt.Sprintf("insert into %s(%s)values(%s)",
			*table, strings.Join(cols, ",\n"), strings.Join(pnames, ",\n"))

	case "postgres":
		pnames := []string{}
		for i, _ := range cols {
			pnames = append(pnames, fmt.Sprintf("$%d", i))
		}
		strSql = fmt.Sprintf("insert into %s(%s)values(%s)",
			*table, strings.Join(cols, ",\n"), strings.Join(pnames, ",\n"))
	}

	db, err := sqlx.Open(*driver, *dburl)
	if err != nil {
		log.Panic(err)
	}
	defer db.Close()
	if *truncate {
		if _, err = db.Exec("truncate table " + *table); err != nil {
			log.Panic(err)
		}
	}
	tx, err := db.Beginx()
	if err != nil {
		log.Panic(err)
	}
	defer func() {
		if r := recover(); r != nil {
			tx.Rollback()
		}
	}()
	stmt, err := db.Prepare(strSql)
	if err != nil {
		log.Panic(err)
	}
	defer func() {
		stmt.Close()
	}()
	var rn uint64 = 0
	startTime := time.Now()
	preTime := startTime
	var preCount uint64
	for {
		rn++
		out := []interface{}{}
		if err = dec.Decode(&out); err == io.EOF {
			break
		} else if err != nil {
			log.WithFields(log.Fields{
				"no": rn,
			}).Panic(err)
		}
		if _, err = stmt.Exec(out...); err != nil {
			log.WithFields(log.Fields{
				"no": rn,
			}).Panic(err)
		}
		if time.Since(preTime).Seconds() > 15 {
			log.WithFields(log.Fields{
				"no": rn,
				"op": fmt.Sprintf("%.0f", float64(rn-preCount)/time.Since(preTime).Seconds()),
			}).Info("progress")
			preTime = time.Now()
			preCount = rn
			if err = tx.Commit(); err != nil {
				log.Panic(err)
			}
			if err = stmt.Close(); err != nil {
				log.Panic(err)
			}
			if tx, err = db.Beginx(); err != nil {
				log.Panic(err)
			}
			stmt, err = tx.Prepare(strSql)
		}
	}
	if err = tx.Commit(); err != nil {
		log.Panic(err)
	}

	log.WithFields(log.Fields{
		"table": *table,
		"file":  *filename,
		"rows":  rn,
	}).Info("finish")
}
