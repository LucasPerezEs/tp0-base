package common

import (
	"bufio"
	"fmt"
	"net"
	"time"
	"os"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

// ClientConfig Configuration used by the client
type ClientConfig struct {
	ID            string
	ServerAddress string
	LoopAmount    int
	LoopPeriod    time.Duration
	Name          string
	Surname       string
	DNI           string
	BirthDate     string
	BetNumber     int
}

// Client Entity that encapsulates how
type Client struct {
	config ClientConfig
	conn   net.Conn
}

// NewClient Initializes a new client receiving the configuration
// as a parameter
func NewClient(config ClientConfig) *Client {
	client := &Client{
		config: config,
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


func (c *Client) StartClientLoop(signalChannel chan os.Signal) {

	// There is an autoincremental msgID to identify every message sent
	// Messages if the message amount threshold has not been surpassed
	for msgID := 1; msgID <= c.config.LoopAmount; msgID++ {
		select {

		case <-signalChannel:
			log.Infof("action: shutdown | result: success | client_id: %v", c.config.ID)
			return

		default:
			// Create the connection the server in every loop iteration.
			if err := c.createClientSocket(); err != nil {
				time.Sleep(c.config.LoopPeriod) // Wait before retrying to avoid busy loop
				return
			}

			// TODO: Modify the send to avoid short-write
			if _, err := fmt.Fprintf(c.conn, "[CLIENT %v] Message N°%v\n", c.config.ID, msgID); err != nil {
				log.Errorf("action: send_message | result: fail | client_id: %v | error: %v", c.config.ID, err)
				return
			}
			msg, err := bufio.NewReader(c.conn).ReadString('\n')
			c.conn.Close()

			if err != nil {
				log.Errorf("action: receive_message | result: fail | client_id: %v | error: %v",
					c.config.ID,
					err,
				)
				return
			}

			log.Infof("action: receive_message | result: success | client_id: %v | msg: %v",
				c.config.ID,
				msg,
			)

			// Wait a time between sending one message and the next one
			time.Sleep(c.config.LoopPeriod)
		}
	}

	log.Infof("action: loop_finished | result: success | client_id: %v", c.config.ID)
}