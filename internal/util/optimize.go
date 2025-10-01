package util

import (
	"maps"
	"slices"
)

func CombineRecordsWithSamePrimaryKey(records []*Record) []*Record {
	recordsMap := make(map[string][]*Record)
	for _, record := range records {
		key := record.Table + record.Id
		recordsMap[key] = append(recordsMap[key], record)
	}

	combine := func(records []*Record) []*Record {
		if len(records) == 1 {
			return records
		}
		collapsedRecords := []*Record{records[0]}
		previousRecord := collapsedRecords[0]
		for _, record := range records[1:] {
			type batchCondition int
			const (
				withoutBatch batchCondition = iota
				updateWithBatch
				deleteWithBatch
			)

			batchType := withoutBatch
			switch record.Operation {
			case "DELETE":
				batchType = deleteWithBatch
			case "UPDATE":
				switch previousRecord.Operation {
				case "INSERT":
					batchType = withoutBatch
				case "UPDATE":
					batchType = updateWithBatch
				}
			}

			switch batchType {
			case withoutBatch:
				collapsedRecords = append(collapsedRecords, record)
				previousRecord = record
			case updateWithBatch:
				for _, key := range record.Diff {
					if !slices.Contains(previousRecord.Diff, key) {
						previousRecord.Diff = append(previousRecord.Diff, key)
					}
				}
				previousRecord.Event = record.Event
				maps.Copy(previousRecord.Object, record.Object)
			case deleteWithBatch:
				collapsedRecords = collapsedRecords[:0]
				collapsedRecords = append(collapsedRecords, record)
				previousRecord = record
			}
		}
		return collapsedRecords
	}

	var processedRecords []*Record
	for _, records := range recordsMap {
		processedRecords = append(processedRecords, combine(records)...)
	}

	return processedRecords
}
