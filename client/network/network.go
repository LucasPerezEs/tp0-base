package network

import (
	"fmt"
	"net"
	"io"
)

// SendFrameWithACK envía un frame con formato: 1 byte type, 2 bytes length (big-endian), payload.
// Luego lee 1 byte ACK y lo devuelve.
func SendFrameWithACK(conn net.Conn, frame []byte) (byte, error) {

    if len(frame) > 0xFFFF {
        return 0, fmt.Errorf("payload too large for uint16")
    }

	if err := SendAll(conn, frame); err != nil {
		return 0, err
	}

    // receive ACK
    ack, err := ReceiveACK(conn)
    if err != nil {
        return 0, err
    }
    return ack, nil
}


// SendAll ensures that all bytes in the data slice are sent through the connection. 
func SendAll(conn net.Conn, data []byte) error {
	totalSent := 0
	for totalSent < len(data) {
		n, err := conn.Write(data[totalSent:])
		if err != nil {
			return err
		}
		totalSent += n
	}
	return nil
}


// ReceiveACK reads a single byte from the connection, which is expected to be an ACK from the server.
func ReceiveACK(conn net.Conn) (byte, error) {
    var ackBuf [1]byte
    if _, err := io.ReadFull(conn, ackBuf[:]); err != nil {
        return 0, err
    }
    return ackBuf[0], nil
}


// waitForResults reads a RESULTS frame (type=0x04) and ACKs it.
// Frame = 1 byte type | 2 bytes len | payload: [2 bytes len][document]...
func (c *Client) waitForResults() error {
    // read header (type + len)
    header := make([]byte, 3)
    if _, err := io.ReadFull(c.conn, header); err != nil {
        return fmt.Errorf("read_results_header: %w", err)
    }

    frameType := header[0]
    if frameType != 0x04 {
        return fmt.Errorf("unexpected frame type: %02x", frameType)
    }

    payloadLen := int(binary.BigEndian.Uint16(header[1:3]))
    payload := make([]byte, payloadLen)
    if payloadLen > 0 {
        if _, err := io.ReadFull(c.conn, payload); err != nil {
            return fmt.Errorf("read_results_payload: %w", err)
        }
    }

    // parse winners: repeated [uint16 len][document bytes]
    var winners []string
    off := 0
    for off < len(payload) {
        if off+2 > len(payload) {
            return fmt.Errorf("parse_results: short length")
        }
        ln := int(binary.BigEndian.Uint16(payload[off : off+2]))
        off += 2
        if off+ln > len(payload) {
            return fmt.Errorf("parse_results: short document")
        }
        winners = append(winners, string(payload[off:off+ln]))
        off += ln
    }

    log.Infof("action: consulta_ganadores | result: success | cant_ganadores: %d", len(winners))

    // ACK the server for the results
    if _, err := c.conn.Write([]byte{0x01}); err != nil {
        return fmt.Errorf("send_ack_results: %w", err)
    }
    return nil
}