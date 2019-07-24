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
	ctx    context.Context
	client *firestore.Client
)

const collectionId = "sample"

func addJsonDoc(docID string) {
	fmt.Println("Writing json data")
	bytes, _ := ioutil.ReadFile("./single-property.json")

	var data interface{}
	json.Unmarshal(bytes, &data)

	writeResult, err := client.Collection(collectionId).Doc(docID).Create(ctx, data)

	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(writeResult.UpdateTime.String())
}

func queryJsonDoc(docID string) {
	fmt.Println("Querying json data")
	doc, err := client.Collection(collectionId).Doc(docID).Get(ctx)

	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(doc.Data())
}

func addStructAsDoc()  {
	fmt.Println("Add custom struct data as document")
	data := struct{
		PropertyId string `json:"property_id"`
		Name string
	}{
		"98765", "Hyatt",
	}

	result, err := client.Collection(collectionId).Doc("custom-struct").Create(ctx, data)

	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(result.UpdateTime.String())
}

func main() {
	ctx = context.Background()
	var e error
	client, e = firestore.NewClient(ctx, "golearning-qa")

	if e != nil {
		log.Fatal(e)
	}
	defer client.Close()

	//docID := "json-1"
	//addJsonDoc(docID)
	//queryJsonDoc(docID)
	addStructAsDoc()
}
