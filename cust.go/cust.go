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
	baslangic := time.Now()

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

	collection := client.Database("first-pars").Collection("first-chunk")

	totalRecords := 0

	chunk := 1000

	for {

		var records []interface{}

		for i := 0; i < chunk; i++ {
			line, err := reader.Read()
			if err != nil {
				break
			}

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

			records = append(records, record)
		}

		if len(records) == 0 {
			break
		}

		_, err = collection.InsertMany(ctx, records)
		if err != nil {
			log.Fatal(err)
		}

		totalRecords += len(records)

		fmt.Printf("%d İşlem Tamamlandı.\n", totalRecords)
	}

	bitis := time.Now()

	fmt.Println("CSV dosyanız MongoDB üzerine aktarılmıştır.")
	fmt.Println("Başlangıç:", baslangic)
	fmt.Println("Bitiş:", bitis)
}
