package common

import (
	"net"
	"os"
	"encoding/csv"
	"bytes"
	"io"

	"github.com/op/go-logging"
	"github.com/7574-sistemas-distribuidos/docker-compose-init/client/protocol"
	"github.com/7574-sistemas-distribuidos/docker-compose-init/client/network"
	"github.com/7574-sistemas-distribuidos/docker-compose-init/client/domain"
)

var log = logging.MustGetLogger("log")

// ClientConfig Configuration used by the client
type ClientConfig struct {
	ID             uint8
	ServerAddress  string
	BatchMaxAmount int
	BatchMaxKb     int
	DataPath       string
}

// Client Entity that encapsulates how the client interacts with the server
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

// sendBatch envía batchBuf si tiene contenido y espera ACK.
func (c *Client) sendBatch(batchBuf *bytes.Buffer, batchCount *int, batchBytes *int) error {
    if *batchCount == 0 {
        return nil
    }
    if err := network.SendAll(c.conn, batchBuf.Bytes()); err != nil {
        log.Errorf("action: send_batch | result: fail | client_id: %v | error: %v", c.config.ID, err)
        return err
    }
    ack, err := network.ReceiveACK(c.conn)
    if err != nil {
        log.Errorf("action: receive_ack | result: fail | client_id: %v | error: %v", c.config.ID, err)
        return err
    }
    if ack != 1 {
        log.Warningf("action: receive_ack | result: negative | client_id: %v | ack: %02x", c.config.ID, ack)
    }
    batchBuf.Reset()
    *batchCount = 0
    *batchBytes = 0
    return nil
}

// appendToBatch añade message al batch, enviando el batch existente si hace falta.
func (c *Client) appendToBatch(batchBuf *bytes.Buffer, message []byte, batchCount *int, batchBytes *int, maxBytes int) error {
    // Si al añadir este mensaje se exceden los límites, enviamos el batch actual primero.
    if *batchCount > 0 && (*batchCount+1 > c.config.BatchMaxAmount || (maxBytes > 0 && *batchBytes+len(message) > maxBytes)) {
        if err := c.sendBatch(batchBuf, batchCount, batchBytes); err != nil {
            return err
        }
    }

    // Añadir el mensaje al batch
    if _, err := batchBuf.Write(message); err != nil {
        log.Errorf("action: batch_write | result: fail | client_id: %v | error: %v", c.config.ID, err)
        return err
    }
    *batchCount++
    *batchBytes += len(message)

    // Si alcanzamos el límite exacto, enviamos inmediatamente
    if *batchCount >= c.config.BatchMaxAmount || (maxBytes > 0 && *batchBytes >= maxBytes) {
        if err := c.sendBatch(batchBuf, batchCount, batchBytes); err != nil {
            return err
        }
    }
    return nil
}

// processBets lee el CSV y delega en appendToBatch. Retorna error si ocurre fallo I/O serio.
func (c *Client) processBets(f io.Reader) error {
    var batchBuf bytes.Buffer
    batchCount := 0
    batchBytes := 0
    maxBytes := c.config.BatchMaxKb * 1024

    reader := csv.NewReader(f)
    for {
        record, err := reader.Read()
        if err != nil {
            if err == io.EOF {
                break
            }
            return err
        }

        bet, err := domain.ParseBet(record)
        if err != nil {
            log.Errorf("action: parse_bet | result: fail | client_id: %v | error: %v", c.config.ID, err)
            continue
        }

        message, err := protocol.SerializeBet(c.config.ID, bet)
        if err != nil {
            log.Errorf("action: serialize_bet | result: fail | client_id: %v | error: %v", c.config.ID, err)
            continue
        }

        if err := c.appendToBatch(&batchBuf, message, &batchCount, &batchBytes, maxBytes); err != nil {
            return err
        }
    }

    // Enviar batch restante si queda
    if err := c.sendBatch(&batchBuf, &batchCount, &batchBytes); err != nil {
        return err
    }
    return nil
}

// StartClient ahora solo orquesta: crear socket, abrir archivo y llamar a processBets.
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

	// Abrir archivo (propio ownership). Si no existe, loguear y cerrar conexión.
	f, err := os.Open(c.config.DataPath)
	if err != nil {
		log.Infof("action: process_dataset | result: no_file | client_id: %v | path: %s", c.config.ID, c.config.DataPath)
		_ = c.conn.Close()
		return
	}
	defer f.Close()

	if err := c.processBets(f); err != nil {
		log.Errorf("action: process_bets | result: fail | client_id: %v | error: %v", c.config.ID, err)
		_ = c.conn.Close()
		return
	}

	// Cerrar conexión después de terminar
	if err := c.conn.Close(); err != nil {
		log.Errorf("action: shutdown | result: fail | client_id: %v | error: %v", c.config.ID, err)
	} else {
		log.Infof("action: shutdown | result: success | client_id: %v", c.config.ID)
	}
}