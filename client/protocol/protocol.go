package protocol

import (
	"encoding/binary"
	"bytes"
	"errors"
	"github.com/7574-sistemas-distribuidos/docker-compose-init/client/domain"
)


func SerializeBet(id uint8, b domain.Bet) ([]byte, error) {
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

    // Create the full message with length prefix
    full := bytes.Buffer{}
    if payload.Len() > 0xFFFF {
        return nil, errors.New("payload too large for uint16 size")
    }
    if err := binary.Write(&full, binary.BigEndian, uint16(payload.Len())); err != nil {
        return nil, err
    }
    if _, err := full.Write(payload.Bytes()); err != nil {
        return nil, err
    }
    return full.Bytes(), nil
}