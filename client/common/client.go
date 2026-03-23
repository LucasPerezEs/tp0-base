package common

import (
	"net"
	"os"
	"time"

	"github.com/op/go-logging"
	"github.com/7574-sistemas-distribuidos/docker-compose-init/client/protocol"
	"github.com/7574-sistemas-distribuidos/docker-compose-init/client/network"
	"github.com/7574-sistemas-distribuidos/docker-compose-init/client/domain"
)

var log = logging.MustGetLogger("log")


// ClientConfig Configuration used by the client
type ClientConfig struct {
	ID            uint8
	ServerAddress string
	LoopAmount    int
	LoopPeriod    time.Duration
}

// Client Entity that encapsulates how the client interacts with the server
type Client struct {
    config ClientConfig
    bet    domain.Bet
    conn   net.Conn
}

// NewClient Initializes a new client receiving the configuration
// as a parameter
func NewClient(config ClientConfig, bet domain.Bet) *Client {
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

	// Receive ACK from the server
	ack, err := network.ReceiveACK(c.conn)
	if err != nil {
		log.Errorf("action: receive_ack | result: fail | client_id: %v | error: %v", c.config.ID, err)
		return
	}
	
	if ack != 1 {
		log.Warningf("action: receive_ack | result: fail | client_id: %v | error: invalid ACK value %v", c.config.ID, ack)
		return
	}

	log.Infof("action: apuesta_enviada | result: success | dni: %s | numero: %d",
		c.bet.Document,
		c.bet.Number,
	)
}