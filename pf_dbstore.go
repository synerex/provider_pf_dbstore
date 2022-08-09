package main

import (
	// "encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"database/sql"

	protoPF "github.com/UCLabNU/proto_pflow"
	_ "github.com/go-sql-driver/mysql"

	"github.com/golang/protobuf/proto"
	api "github.com/synerex/synerex_api"
	pbase "github.com/synerex/synerex_proto"

	sxutil "github.com/synerex/synerex_sxutil"
	//sxutil "local.packages/synerex_sxutil"

	"log"
	"sync"
)

// datastore provider provides Datastore Service.

var (
	nodesrv         = flag.String("nodesrv", "127.0.0.1:9990", "Node ID Server")
	local           = flag.String("local", "", "Local Synerex Server")
	mu              sync.Mutex
	version         = "0.01"
	baseDir         = "store"
	dataDir         string
	pfMu            *sync.Mutex = nil
	pfLoop          *bool       = nil
	ssMu            *sync.Mutex = nil
	ssLoop          *bool       = nil
	sxServerAddress string
	currentNid      uint64                  = 0 // NotifyDemand message ID
	mbusID          uint64                  = 0 // storage MBus ID
	storageID       uint64                  = 0 // storageID
	pfClient        *sxutil.SXServiceClient = nil
	db              *sql.DB
	db_host         = os.Getenv("MYSQL_HOST")
	db_name         = os.Getenv("MYSQL_DATABASE")
	db_user         = os.Getenv("MYSQL_USER")
	db_pswd         = os.Getenv("MYSQL_PASSWORD")
)

const layout = "2006-01-02T15:04:05.999999Z"
const layout_db = "2006-01-02 15:04:05.999"

func init() {
	// connect
	addr := fmt.Sprintf("%s:%s@(%s:3306)/%s", db_user, db_pswd, db_host, db_name)
	print("connecting to " + addr + "\n")
	var err error
	db, err = sql.Open("mysql", addr)
	if err != nil {
		print("connection error: ")
		print(err)
		print("\n")
	}

	// ping
	err = db.Ping()
	if err != nil {
		print("ping error: ")
		print(err)
		print("\n")
	}

	// create table
	_, err = db.Exec(`create table if not exists pf(id BIGINT unsigned not null auto_increment, src_time DATETIME(3) not null, src_sid INT unsigned not null, dst_time DATETIME(3) not null, dst_sid INT unsigned not null, primary key(id))`)
	// select hex(mac) from log;
	// insert into pf (mac) values (x'000CF15698AD');
	if err != nil {
		print("exec error: ")
		print(err)
		print("\n")
	}
}

func dbStore(src_ts time.Time, src_sid uint32, dst_ts time.Time, dst_sid uint32) {

	// ping
	err := db.Ping()
	if err != nil {
		print("ping error: ")
		print(err)
		print("\n")
		// connect
		addr := fmt.Sprintf("%s:%s@(%s:3306)/%s", db_user, db_pswd, db_host, db_name)
		print("connecting to " + addr + "\n")
		db, err = sql.Open("mysql", addr)
		if err != nil {
			print("connection error: ")
			print(err)
			print("\n")
		}
	}

	log.Printf("Storeing %v, %s, %s, %d, %s, %d", ts.Format(layout_db), hexmac, hostname, sid, dir, height)
	result, err := db.Exec(`insert into pf(time, mac, hostname, sid, dir, height) values(?, ?, ?, ?, ?, ?)`, ts.Format(layout_db), nummac, hostname, sid, dir, height)

	if err != nil {
		print("exec error: ")
		print(err)
		print("\n")
	} else {
		rowsAffected, err := result.RowsAffected()
		if err != nil {
			print(err)
		} else {
			print(rowsAffected)
		}
	}

}

// called for each agent data.
func supplyPFlowCallback(clt *sxutil.SXServiceClient, sp *api.Supply) {

	if sp.SupplyName == "PFlow Supply" {
		pf := &protoPF.PFlow{}
		err := proto.Unmarshal(sp.Cdata.Entity, pf)
		if err == nil {
			dbStore(pf.Operation[0].Timestamp.AsTime(), uint32(pf.Operation[0].Sid), uint32(pf.Operation[0].Height), pf.Operation[1].Timestamp.AsTime(), uint32(pf.Operation[1].Sid), uint32(pf.Operation[1].Height))
		} else {
			log.Printf("Unmarshaling err PF: %v", err)
		}
	} else {
		log.Printf("Received Unknown (%4d bytes)", len(sp.Cdata.Entity))
	}

}

func main() {
	flag.Parse()
	go sxutil.HandleSigInt()
	sxutil.RegisterDeferFunction(sxutil.UnRegisterNode)
	log.Printf("PF-dbstore(%s) built %s sha1 %s", sxutil.GitVer, sxutil.BuildTime, sxutil.Sha1Ver)

	channelTypes := []uint32{pbase.PEOPLE_FLOW_SVC, pbase.STORAGE_SERVICE}

	var rerr error
	sxServerAddress, rerr = sxutil.RegisterNode(*nodesrv, "PFdbStore", channelTypes, nil)

	if rerr != nil {
		log.Fatal("Can't register node:", rerr)
	}
	if *local != "" { // quick hack for AWS local network
		sxServerAddress = *local
	}
	log.Printf("Connecting SynerexServer at [%s]", sxServerAddress)

	wg := sync.WaitGroup{} // for syncing other goroutines

	client := sxutil.GrpcConnectServer(sxServerAddress)

	if client == nil {
		log.Fatal("Can't connect Synerex Server")
	}

	pfClient = sxutil.NewSXServiceClient(client, pbase.PEOPLE_FLOW_SVC, "{Client:PFdbStore}")

	log.Print("Subscribe PFount Supply")
	pfMu, pfLoop = sxutil.SimpleSubscribeSupply(pfClient, supplyPFlowCallback)

	wg.Add(1)
	wg.Wait()
}
