package protocol

import (
	"encoding/binary"
	"bytes"
	"errors"
	"github.com/7574-sistemas-distribuidos/docker-compose-init/client/common"
	""
)


func SerializeBet(id uint8, bet common.Bet) ([]byte, error) {
	    var payload bytes.Buffer

    // ID
    if err := binary.Write(&payload, binary.BigEndian, id); err != nil {
        return nil, err
    }

    if err := WriteStr(&payload, b.Name); err != nil {
        return nil, err
    }
    if err := WriteStr(&payload, b.Surname); err != nil {
        return nil, err
    }
    if err := WriteStr(&payload, b.DNI); err != nil {
        return nil, err
    }
    if err := WriteStr(&payload, b.BirthDate); err != nil {
        return nil, err
    }

    if b.BetNumber < 0 || b.BetNumber > 0xFFFF {
        return nil, errors.New("bet number out of range for uint16")
    }
    if err := binary.Write(&payload, binary.BigEndian, uint16(b.BetNumber)); err != nil {
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