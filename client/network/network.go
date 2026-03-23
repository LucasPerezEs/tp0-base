package network

import (
	"net"
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