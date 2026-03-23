package domain

import (
	"fmt"
	"strconv"
)


// Bet Struct that encapsulates the bet information
type Bet struct {
	FirstName string
	LastName  string
	Document  string
	Birthdate string
	Number    int
}

// ParseBet Receives a record of the csv file and parses it to a Bet struct. If the record is not valid, an error is returned
func ParseBet(record []string) (Bet, error) {
	if len(record) != 5 {
		return Bet{},  fmt.Errorf("invalid record length: expected 5 fields, got %d", len(record))
	}

	number, err := strconv.Atoi(record[4])
	if err != nil {
		return Bet{}, fmt.Errorf("invalid number field: %v", err)
	}

	return Bet{
		FirstName: record[0],
		LastName:  record[1],
		Document:  record[2],
		Birthdate: record[3],
		Number:    number,
	}, nil
}