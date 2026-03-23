package network

import (
	"fmt"
	"net"
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