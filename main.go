package main

import (
	"code.google.com/p/go-sqlite/go1/sqlite3"
	"flag"
	"fmt"
	"log"
	"os"
	"time"
)

const heatpumpDB = "/home/mhusmann/Documents/src/pyt/heizung/heatpump.db"

// const heatpumpDB = "heatpump.db"
const nanoSecondsDay = 86400000000000
const timeShortForm = "2006-01-02"
const nanoSDay = 86400000000000

type dataSet struct {
	dbName           *string
	sql              *sqlite3.Conn
	dayDb            string
	day              *string
	htDb, ntDb       int64
	numOfDays        int64
	diffHt, diffNt   int64
	daylyHt, daylyNt int64
	restHt, restNt   int64
	ht, nt           *int64
	tarifId          *int64
}

func (e *dataSet) init() {
	e.dayDb, e.htDb, e.ntDb = lastEntry(e.sql)
	fmt.Printf("# Datenbank:       %s, HT: %6d, NT: %6d\n",
		e.dayDb, e.htDb, e.ntDb)
	if *e.day == "" {
		t := time.Now()
		*e.day = fmt.Sprintf("%d-%02d-%02d",
			t.Year(), t.Month(), t.Day())
	}
	fmt.Printf("# Aktuelles Datum: %s, HT: %6d, NT: %6d\n",
		*e.day, *e.ht, *e.nt)

	e.numOfDays = e.days()
	if e.numOfDays <= 0 {
		log.Fatal("# die Datenbank scheint auf dem aktuellen Stand zu sein")
	}
	fmt.Printf("# %d Tage zum letztem Eintrag\n",
		e.numOfDays)

	if *e.ht < e.htDb || *e.nt < e.ntDb {
		log.Fatalf("# Ht: %d oder NT: %d können nicht kleiner sein, als "+
			"die Datenbankwerte\n", *e.ht, *e.nt)
	}

	e.diffHt, e.diffNt = *e.ht-e.htDb, *e.nt-e.ntDb
	fmt.Printf("# Verbrauch HT: %d, NT: %d\n", e.diffHt, e.diffNt)
	e.daylyHt, e.restHt = e.diffHt/e.numOfDays, e.diffHt%e.numOfDays
	e.daylyNt, e.restNt = e.diffNt/e.numOfDays, e.diffNt%e.numOfDays
	fmt.Printf("# Täglich   Ht: %d, Rest: %d NT: %d, Rest: %d\n",
		e.daylyHt, e.restHt, e.daylyNt, e.restNt)

	// log.Fatalf("tarif Id = %d\n", *e.tarifId)
}

// calculate the difference between two dates.
func (e *dataSet) days() int64 {
	d1, err := time.Parse(timeShortForm, *e.day)
	if err != nil {
		log.Fatal(err)
	}
	d2, err := time.Parse(timeShortForm, e.dayDb)
	if err != nil {
		log.Fatal(err)
	}
	d := d1.Sub(d2)
	d = d / time.Duration(nanoSDay) * time.Nanosecond
	return int64(d / time.Nanosecond)
}

// now iterate from the last date + 1day in the db to the current date
// inserting the amounts for Ht and Nt values + restHt and restNt
func (e *dataSet) insertValues() {
	baseDay, err := time.Parse(timeShortForm, e.dayDb)
	if err != nil {
		log.Fatal(err)
	}
	var addHt, addNt int64
	var day string
	var i int64
	for i = 1; i <= e.numOfDays; i++ {
		baseDay = baseDay.Add(nanoSDay)
		day = fmt.Sprintf("%4d-%02d-%02d",
			baseDay.Year(), baseDay.Month(), baseDay.Day())
		addHt = 0
		if e.restHt > 0 {
			addHt = 1
			e.restHt -= 1
		}
		addNt = 0
		if e.restNt > 0 {
			addNt = 1
			e.restNt -= 1
		}
		e.htDb += e.daylyHt + addHt
		e.ntDb += e.daylyNt + addNt
		args := sqlite3.NamedArgs{"$day": day,
			"$ht": e.htDb, "$nt": e.ntDb, "$tarifId": *e.tarifId}
		e.sql.Exec("INSERT INTO dayly VALUES($x,$day,$ht,$nt,$tarifId)", args)
	}
	fmt.Printf("# Inserted %d entries into %s\n", i, *e.dbName)
}

func lastEntry(sql *sqlite3.Conn) (string, int64, int64) {
	dbResult, err := sql.Query("select max(day), max(ht), " +
		"max(nt) from dayly")
	if err != nil {
		log.Fatal(err)
	}
	var day string
	var ht int64
	var nt int64
	dbResult.Scan(&day, &ht, &nt)
	return day, ht, nt
}

func main() {
	dbName := flag.String("dbName", heatpumpDB,
		"Pfad und Name der Datenbank")
	day := flag.String("heute", "",
		"Aktuelles Datum in Iso wie 2015-05-22")
	ht := flag.Int64("ht", 0, "Aktueller HT Wert")
	nt := flag.Int64("nt", 0, "Aktueller NT Wert")
	tarifId := flag.Int64("tarifId", 1, "Tarif ID")
	flag.Parse()

	if _, err := os.Stat(*dbName); os.IsNotExist(err) {
		log.Fatalf("# Die Datenbank: %s existiert nicht\n",
			*dbName)
	}
	sql, err := sqlite3.Open(*dbName)
	if err != nil {
		log.Fatalf("# Kann Datenbank: %s nicht öffnen: %s\n",
			*dbName, err)
	}
	defer sql.Close()

	e := dataSet{sql: sql, dbName: dbName, day: day, ht: ht, nt: nt, tarifId: tarifId}
	e.init()
	e.insertValues()
}
