package common

import (
	"bufio"
	"fmt"
	"net"
	"time"
	"os"

	"github.com/op/go-logging"
	"github.com/7574-sistemas-distribuidos/docker-compose-init/client/protocol/protocol"
	"github.com/7574-sistemas-distribuidos/docker-compose-init/client/network"
)

var log = logging.MustGetLogger("log")

// Bet Struct that encapsulates the bet information
type Bet struct {
	Name      string
	Surname   string
	DNI       string
	BirthDate string
	BetNumber int
}

// ClientConfig Configuration used by the client
type ClientConfig struct {
	ID            string
	ServerAddress string
	LoopAmount    int
	LoopPeriod    time.Duration
}

// Client Entity that encapsulates how the client interacts with the server
type Client struct {
	config ClientConfig
	bet    Bet
	conn   net.Conn
}

// NewClient Initializes a new client receiving the configuration
// as a parameter
func NewClient(config ClientConfig, bet Bet) *Client {
	client := &Client{
		config: config,
		bet:    bet,
	}
	return client
}

// CreateClientSocket Initializes client socket. In case of
// failure, error is printed in stdout/stderr and returned to the caller
func (c *Client) createClientSocket() error {
	conn, err := net.Dial("tcp", c.config.ServerAddress)
	if err != nil {
		log.Criticalf(
			"action: connect | result: fail | client_id: %v | error: %v",
			c.config.ID,
			err,
		)
		return err
	}
	c.conn = conn
	return nil
}


func (c *Client) StartClient(signalChannel chan os.Signal) {

	go func() {
		<-signalChannel
		log.Infof("action: shutdown | result: in_progress | client_id: %v", c.config.ID)
		if err := c.conn.Close(); err != nil {
			log.Errorf("action: shutdown | result: fail | client_id: %v | error: %v", c.config.ID, err)
		} else {
			log.Infof("action: shutdown | result: success | client_id: %v", c.config.ID)
		}
		os.Exit(0)
	}()

	// Create the connection the server before starting the loop.
	if err := c.createClientSocket(); err != nil {
		log.Errorf("action: connect | result: fail | client_id: %v | error: %v", c.config.ID, err)
		return
	}

	// Create the message to send to the server with the bet information
	message, err := protocol.SerializeBet(c.config.ID, c.bet)
	if err != nil {
		log.Errorf("action: serialize_bet | result: fail | client_id: %v | error: %v", c.config.ID, err)
		return
	}

	// Send the message to the server
	if err := network.SendAll(c.conn, message); err != nil {
		log.Errorf("action: send_bet | result: fail | client_id: %v | error: %v", c.config.ID, err)
		return
	}

	// TODO: Receive ACK from the server

	log.Infof("action: apuesta_enviada | result: success | dni: %s | numero: %d",
		c.bet.DNI,
		c.bet.BetNumber,
	)
}