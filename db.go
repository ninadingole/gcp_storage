package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"

	"cloud.google.com/go/firestore"
)

var (
	ctx context.Context
	client *firestore.Client
)

func addJsonDoc() {
	fmt.Println("Writing json data")
	bytes, _ := ioutil.ReadFile("./single-property.json")

	var data interface{}
	json.Unmarshal(bytes, &data)

	writeResult, err := client.Collection("sample").Doc("json-1").Create(ctx, data)

	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(writeResult.UpdateTime.String())
}

func queryJsonDoc() {
	fmt.Println("Querying json data")
	doc, err := client.Collection("sample").Doc("json-1").Get(ctx)

	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(doc.Data())
}

func main() {
	ctx = context.Background()
	var e error
	client, e = firestore.NewClient(ctx, "golearning-qa")

	if e != nil {
		log.Fatal(e)
	}
	defer client.Close()

	addJsonDoc()
	queryJsonDoc()
}
