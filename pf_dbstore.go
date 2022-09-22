package main

import (
	// "encoding/json"
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	protoPF "github.com/UCLabNU/proto_pflow"
	_ "github.com/go-sql-driver/mysql"

	"github.com/golang/protobuf/proto"
	"github.com/jackc/pgx/v4"
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
	db              *pgx.Conn
	db_host         = os.Getenv("POSTGRES_HOST")
	db_name         = os.Getenv("POSTGRES_DB")
	db_user         = os.Getenv("POSTGRES_USER")
	db_pswd         = os.Getenv("POSTGRES_PASSWORD")
)

const layout = "2006-01-02T15:04:05.999999Z"
const layout_db = "2006-01-02 15:04:05.999"

func init() {
	// connect
	ctx := context.Background()
	addr := fmt.Sprintf("postgres://%s:%s@%s:5432/%s", db_user, db_pswd, db_host, db_name)
	print("connecting to " + addr + "\n")
	var err error
	db, err = pgx.Connect(ctx, addr)
	if err != nil {
		print("connection error: ")
		log.Println(err)
		log.Fatal("\n")
	}
	defer db.Close(ctx)

	// ping
	err = db.Ping(ctx)
	if err != nil {
		print("ping error: ")
		log.Println(err)
		log.Fatal("\n")
	}

	// create table
	_, err = db.Exec(ctx, `create table if not exists pf(id BIGSERIAL NOT NULL, src_time TIMESTAMP not null, src_sid INT not null, dst_time TIMESTAMP not null, dst_sid INT not null, primary key(id))`)
	// select hex(mac) from log;
	// insert into pf (mac) values (x'000CF15698AD');
	if err != nil {
		print("create table error: ")
		log.Println(err)
		log.Fatal("\n")
	}
}

func dbStore(src_ts time.Time, src_sid uint32, dst_ts time.Time, dst_sid uint32) {

	// ping
	ctx := context.Background()
	err := db.Ping(ctx)
	if err != nil {
		print("ping error: ")
		log.Println(err)
		print("\n")
		// connect
		addr := fmt.Sprintf("postgres://%s:%s@%s:5432/%s", db_user, db_pswd, db_host, db_name)
		print("connecting to " + addr + "\n")
		db, err = pgx.Connect(ctx, addr)
		if err != nil {
			print("connection error: ")
			log.Println(err)
			print("\n")
		}
	}

	log.Printf("Storeing %v, %s, %s, %d, %s, %d", src_ts.Format(layout_db), src_sid, dst_ts.Format(layout_db), dst_sid)
	result, err := db.Exec(ctx, `insert into pf(src_time, src_sid, dst_time, dst_sid) values($1, $2, $3, $4)`, src_ts.Format(layout_db), src_sid, dst_ts.Format(layout_db), dst_sid)

	if err != nil {
		print("exec error: ")
		log.Println(err)
		print("\n")
	} else {
		rowsAffected := result.RowsAffected()
		if err != nil {
			log.Println(err)
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
			dbStore(pf.Operation[0].Timestamp.AsTime(), uint32(pf.Operation[0].Sid), pf.Operation[1].Timestamp.AsTime(), uint32(pf.Operation[1].Sid))
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
