package protocol

import (
	"encoding/binary"
	"bytes"
	"errors"
	"fmt"

	"github.com/7574-sistemas-distribuidos/docker-compose-init/client/domain"
)

type FrameType uint8

const (
	FrameTypeData   FrameType = 0x01
	FrameTypeFin FrameType = 0x02
)

// BuildFrame builds external frame: 1 byte for type + 2 bytes for length + payload.
func BuildFrame(t FrameType, payload []byte) ([]byte, error) {
    if len(payload) > 0xFFFF {
        return nil, fmt.Errorf("payload too large for uint16")
    }
    var buf bytes.Buffer
    if err := binary.Write(&buf, binary.BigEndian, uint8(t)); err != nil {
        return nil, err
    }
    if err := binary.Write(&buf, binary.BigEndian, uint16(len(payload))); err != nil {
        return nil, err
    }
    if len(payload) > 0 {
        if _, err := buf.Write(payload); err != nil {
            return nil, err
        }
    }
    return buf.Bytes(), nil
}


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

func SerializeFin() []byte {
	return []byte{uint8(FrameTypeFin), 0x00, 0x00} // type + length=0
}