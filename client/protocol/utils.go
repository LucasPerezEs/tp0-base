package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
)


// WriteStr escribe una cadena con su longitud uint8 seguida de los bytes.
func WriteStr(payload *bytes.Buffer, s string) error {
    sb := []byte(s)
    if len(sb) > 0xFF {
		return errors.New("string too long for uint8 length")

    }
    if err := binary.Write(payload, binary.BigEndian, uint8(len(sb))); err != nil {
        return err
    }
    _, err := payload.Write(sb)
    return err
}