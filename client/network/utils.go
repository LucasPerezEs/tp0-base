package network

import (
	"net"
	"io"
)


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