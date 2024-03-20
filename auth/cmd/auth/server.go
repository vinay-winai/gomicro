package main

import (
	"database/sql"
	"fmt"
	"log"
	"net"
	"os"
	_ "github.com/go-sql-driver/mysql"
	"github.com/vinay-winai/gomicro/internal/implementation/auth"
	pb "github.com/vinay-winai/gomicro/proto"
	"google.golang.org/grpc"
)

const (
	dbDriver = "mysql"
	dbName = "auth"
)

var db *sql.DB

func main() {
	var err error

	dbUser := os.Getenv("MYSQL_USER")
	dbPassword := os.Getenv("MYSQL_PASSWORD")

	// Open a connection to the database
	dsn := fmt.Sprintf("%s:%s@tcp(mysql-auth:3306)/%s", dbUser , dbPassword, dbName)
	db, err = sql.Open(dbDriver, dsn)
	if err != nil {
		log.Fatal(err)
	}
	// check if the database is alive
	defer func() {
		if err = db.Close(); err != nil {
			log.Printf("Error closing database: %s", err)
		}
	}()
	
	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}

	// grpc server setup
	grpcServer := grpc.NewServer()
	authServerImplementation := auth.NewAuthImplementation(db)
	pb.RegisterAuthserviceServer(grpcServer, authServerImplementation)

	// listen and serve
	listener, err := net.Listen("tcp", ":9000")
	if err != nil {
		log.Fatalf("failed to listen on port 9000: %v", err)
	}
	log.Printf("server listening at %v", listener.Addr())
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}


