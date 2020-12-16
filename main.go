package main

import (
	"os"
	"encoding/csv"

	"context"
	"fmt"
	"github.com/rrrkren/topshot-sales/topshot"

	"github.com/onflow/flow-go-sdk/client"
	"google.golang.org/grpc"
)

func handleErr(err error) {
	if err != nil {
		panic(err)
	}
}

func writeCsv(data [][]string) {
    file, err := os.Create("result.csv")
    handleErr(err)
    defer file.Close()

    writer := csv.NewWriter(file)
    defer writer.Flush()

    for _, value := range data {
        err := writer.Write(value)
        handleErr(err)
    }
}

func main() {
	// connect to flow
	flowClient, err := client.New("access-001.mainnet1.nodes.onflow.org:9000", grpc.WithInsecure())
	handleErr(err)
	err = flowClient.Ping(context.Background())
	handleErr(err)

	// fetch latest block
	latestBlock, err := flowClient.GetLatestBlock(context.Background(), false)
	handleErr(err)
	fmt.Println("current height: ", latestBlock.Height)

	// fetch block events of topshot Market.MomentPurchased events for the past 1000 blocks
	blockEvents, err := flowClient.GetEventsForHeightRange(context.Background(), client.EventRangeQuery{
		Type:        "A.c1e4f4f4c4257510.Market.MomentPurchased",
		StartHeight: latestBlock.Height - 10000,
		EndHeight:   latestBlock.Height,
	})
	handleErr(err)

	// Build CSV of fetched data
	var header = []string{"ID", "Price", "Seller", "SerialNumber", "SetID", "SetName", "PlayID", "PlayName", "TransactionID", "BlockHeight"}
	var data = [][]string {header}

	for _, blockEvent := range blockEvents {
		for _, purchaseEvent := range blockEvent.Events {
			// loop through the Market.MomentPurchased events in this blockEvent
			var row = []string{}

			// Purchase Event
			e := topshot.MomentPurchasedEvent(purchaseEvent.Value)
			row = append(row, fmt.Sprint(e.Id()))
			row = append(row, fmt.Sprint(e.Price()))
			row = append(row, fmt.Sprint(e.Seller()))

			saleMoment, err := topshot.GetSaleMomentFromOwnerAtBlock(flowClient, blockEvent.Height-1, *e.Seller(), e.Id())
			handleErr(err)

			// Sale Moment
			row = append(row, fmt.Sprint(saleMoment.SerialNumber()))
			row = append(row, fmt.Sprint(saleMoment.SetID()))
			row = append(row, fmt.Sprint(saleMoment.SetName()))
			row = append(row, fmt.Sprint(saleMoment.PlayID()))
			row = append(row, fmt.Sprint(saleMoment.Play()["FullName"]))

			// Transaction Info
			row = append(row, purchaseEvent.TransactionID.String())
			row = append(row, fmt.Sprint(blockEvent.Height))

			data = append(data, row)
		}
	}

	writeCsv(data)
}