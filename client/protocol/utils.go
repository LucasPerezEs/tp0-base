package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/7574-sistemas-distribuidos/docker-compose-init/client/domain"
)


func BuildPayload(id uint8, b domain.Bet) ([]byte, error) {
	var payload bytes.Buffer

    // ID
    if err := binary.Write(&payload, binary.BigEndian, id); err != nil {
        return nil, err
    }

    if err := WriteStr(&payload, b.FirstName); err != nil {
        return nil, err
    }
    if err := WriteStr(&payload, b.LastName); err != nil {
        return nil, err
    }
    if err := WriteStr(&payload, b.Document); err != nil {
        return nil, err
    }
    if err := WriteStr(&payload, b.Birthdate); err != nil {
        return nil, err
    }

    if b.Number < 0 || b.Number > 0xFFFF {
        return nil, errors.New("bet number out of range for uint16")
    }
    if err := binary.Write(&payload, binary.BigEndian, uint16(b.Number)); err != nil {
        return nil, err
    }

    return payload.Bytes(), nil
}

// WriteStr writes a string to the payload buffer with a uint8 length prefix.
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