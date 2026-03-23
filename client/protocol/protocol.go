package protocol

import (
	"encoding/binary"
	"bytes"
	"errors"
	"github.com/7574-sistemas-distribuidos/docker-compose-init/client/domain"
)

type FrameType uint8

const (
	FrameTypeData   FrameType = 0x01
	FrameTypeFin FrameType = 0x02
)

func SerializeBet(id uint8, b domain.Bet) ([]byte, error) {
    // Build the payload
    payload, err := BuildPayload(id, b)
    if err != nil {
        return nil, err
    }

    if len(payload) > 0xFFFF {
        return nil, errors.New("payload too large for uint16 size")
    }

    // Create the full message with type and length prefix
    var full bytes.Buffer

    // type
    if err := binary.Write(&full, binary.BigEndian, uint8(FrameTypeData)); err != nil {
        return nil, err
    }

    // len
    if err := binary.Write(&full, binary.BigEndian, uint16(len(payload))); err != nil {
        return nil, err
    }

    // payload
    if len(payload) > 0 {
        if _, err := full.Write(payload); err != nil {
            return nil, err
        }
    }

    return full.Bytes(), nil
}

func SerializeFin() []byte {
	return []byte{uint8(FrameTypeFin), 0x00, 0x00} // type + length=0
}