package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Record struct {
	Index            int    `bson:"index"`
	CustomerID       string `bson:"customer_id"`
	FirstName        string `bson:"first_name"`
	LastName         string `bson:"last_name"`
	Company          string `bson:"company"`
	City             string `bson:"city"`
	Country          string `bson:"country"`
	Phone1           string `bson:"phone_1"`
	Phone2           string `bson:"phone_2"`
	Email            string `bson:"email"`
	SubscriptionDate string `bson:"subscription_date"`
	Website          string `bson:"website"`
}

func main() {
	client, err := mongo.NewClient(options.Client().ApplyURI("mongodb://localhost:27017"))
	if err != nil {
		log.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 65*time.Second)
	defer cancel()
	err = client.Connect(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Disconnect(ctx)

	file, err := os.Open("customers-500000.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.FieldsPerRecord = -1
	lines, err := reader.ReadAll()
	if err != nil {
		log.Fatal(err)
	}

	collection := client.Database("first-pars").Collection("first-chunk")

	for _, line := range lines {
		record := Record{}
		record.Index, _ = strconv.Atoi(line[0])
		record.CustomerID = line[1]
		record.FirstName = line[2]
		record.LastName = line[3]
		record.Company = line[4]
		record.City = line[5]
		record.Country = line[6]
		record.Phone1 = line[7]
		record.Phone2 = line[8]
		record.Email = line[9]
		record.SubscriptionDate = line[10]
		record.Website = line[11]
		_, err := collection.InsertOne(ctx, record)
		if err != nil {
			log.Fatal(err)
		}
	}

	fmt.Println("CSV dosyanız MongoDB üzerine aktarılmıştır.")
}
