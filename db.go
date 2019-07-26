package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"strconv"
	"time"

	"cloud.google.com/go/firestore"
	"google.golang.org/api/iterator"
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
	_ = json.Unmarshal(bytes, &data)

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

func queryJsonDocWithRefs(docID string) {
	fmt.Println("Querying json data")
	doc, err := client.Collection(collectionId).Doc(docID).Get(ctx)

	if err != nil {
		log.Fatal(err)
	}

	data := doc.Data()
	ref := data["ref"].(*firestore.DocumentRef)
	snapshot, err := ref.Get(ctx)

	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(snapshot.Data())
}

func addStructAsDoc(docID string) {
	fmt.Println("Add custom struct data as document")
	data := struct {
		PropertyId int16  `firestore:"property_id"`
		Name       string `firestore:"name"`
	}{
		999, "Hyatt",
	}

	result, err := client.Collection(collectionId).Doc(docID).Create(ctx, data)

	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(result.UpdateTime.String())
}

func updateStructAsDoc(docID string) {
	fmt.Println("Update custom struct data in document")

	result, err := client.Collection(collectionId).Doc(docID).Set(ctx, map[string]interface{}{
		"name": "Updated Name",
	}, firestore.MergeAll)

	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(result.UpdateTime.String())
}

func replaceDoc(docID string) {
	fmt.Println("Replacing document")

	result, err := client.Collection(collectionId).Doc(docID).Set(ctx, map[string]interface{}{
		"Name": "Replaced doc",
	})

	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(result.UpdateTime.String())
}

func docRef(docID string, refId string) {
	fmt.Println("Replacing document")

	result := client.Collection(collectionId).Doc(refId)

	writeResult, err := client.Collection(collectionId).Doc(docID).Create(ctx, map[string]interface{}{
		"ref": result,
	})

	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(writeResult.UpdateTime.String())
}

func deleteAll(ids ...string) {
	fmt.Println("Deleting existing docs")
	for _, id := range ids {
		result, err := client.Collection(collectionId).Doc(id).Delete(ctx)
		if err != nil {
			log.Fatal(err)
		}

		fmt.Println(result.UpdateTime.String())
	}
}

func runTransaction(docID string) {
	fmt.Println("Updating doc using transaction")

	ref := client.Collection(collectionId).Doc(docID)
	err := client.RunTransaction(ctx, func(c context.Context, tx *firestore.Transaction) error {
		doc, err := tx.Get(ref)
		if err != nil {
			log.Fatal(err)
		}
		propertyId, err := doc.DataAt("property_id")
		if err != nil {
			log.Fatal(err)
		}

		return tx.Update(ref, []firestore.Update{{Path: "property_id", Value: propertyId.(int64) + 1}})
	})

	if err != nil {
		log.Fatal(err)
	}
}

func queryStructDoc(id string) {
	fmt.Println("Querying doc using query object")

	collection := client.Collection(collectionId)
	q := collection.Where("name", "==", "Test Property Name")
	documents := q.Documents(ctx)
	for {
		doc, e := documents.Next()
		if e == iterator.Done {
			break
		}

		if e != nil {
			log.Fatal(e)
		}

		fmt.Println(doc.Data())
	}
}

func preconditionUpdate(docId string) {
	fmt.Println("Precondition update")

	coll := client.Collection(collectionId)
	doc := coll.Doc(docId)
	snapshot, err := doc.Get(ctx)

	if err != nil {
		log.Fatal(err)
	}

	result, err := doc.Update(ctx,
		[]firestore.Update{{Path: "name", Value: "Updated Name 1"}},
		firestore.LastUpdateTime(snapshot.UpdateTime))

	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(result.UpdateTime.String())
}

func runExamples() {
	deleteAll("json-1", "json-2", "custom-struct", "doc-ref")
	addJsonDoc("json-1")
	queryJsonDoc("json-1")
	addStructAsDoc("custom-struct")
	updateStructAsDoc("custom-struct")
	queryStructDoc("json-1")
	addJsonDoc("json-2")
	replaceDoc("json-2")
	docRef("doc-ref", "json-1")
	queryJsonDocWithRefs("doc-ref")
	runTransaction("custom-struct")
	preconditionUpdate("custom-struct")
}

func main() {
	//_ = os.Setenv("FIRESTORE_EMULATOR_HOST", "localhost:8081")

	ctx = context.Background()
	var e error
	client, e = firestore.NewClient(ctx, "golearning-qa")

	if e != nil {
		log.Fatal(e)
	}
	defer client.Close()

	//benchInsert()
	//benchGet()
	batchUpdates()
}

func batchUpdates(){
	source := rand.NewSource(1)

	bytes, _ := ioutil.ReadFile("./single-property.json")

	var data map[string]interface{}
	_ = json.Unmarshal(bytes, &data)

	collection := client.Collection(collectionId)
	fmt.Println("Starting benchmarks for batch updation")
	startNanos := time.Now().UnixNano()

	for j := 0; j < 100; j++ {
		batch := client.Batch()
		for i := 0; i < 200; i ++ {
			id := strconv.FormatInt(source.Int63(), 10)
			data["property_id"] = id
			batch.Set(collection.Doc(id), data)
		}
		_, err := batch.Commit(ctx)
		if err != nil {
			log.Fatal(err)
		}
	}
	total := (time.Now().UnixNano() - startNanos) / 1000000

	fmt.Printf("Total %v for 20000 docs %v/doc", total, total/100)
}

func benchInsert() {
	source := rand.NewSource(1)

	bytes, _ := ioutil.ReadFile("./single-property.json")

	var data map[string]interface{}
	_ = json.Unmarshal(bytes, &data)

	collection := client.Collection(collectionId)
	fmt.Println("Starting benchmarks for insertion")
	startNanos := time.Now().UnixNano()
	for j := 0; j < 20000; j++ {
		id := strconv.FormatInt(source.Int63(), 10)
		data["property_id"] = id
		_, err := collection.Doc(id).Create(ctx, data)
		if err != nil {
			log.Fatal(err)
		}
	}
	total := (time.Now().UnixNano() - startNanos) / 1000000

	fmt.Printf("Total %v for 1000 docs %v/doc", total, total/20000)
}

func benchGet() {
	source := rand.NewSource(1)
	collection := client.Collection(collectionId)
	fmt.Println("Starting benchmarks for get")

	startNanos := time.Now().UnixNano()
	for j := 0; j < 2000; j++ {
		docIds := make([]*firestore.DocumentRef, 10)
		for i := 0; i < 10; i++ {
			id := strconv.FormatInt(source.Int63(), 10)
			docIds[i] = collection.Doc(id)
		}
		docs, err := client.GetAll(ctx, docIds)
		if err != nil {
			log.Fatal(err)
		}
		for _, doc := range docs {
			_ = doc.Data()
		}
	}
	total := (time.Now().UnixNano() - startNanos) / 1000000

	fmt.Printf("Total %v for 1000 docs %v/doc", total, total/2000)
}
