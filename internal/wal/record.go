package wal

import (
	"bufio"
	"encoding/binary"
	"hash/crc32"
	"io"
)

const (
	recTypeEvent uint8 = 1
	recTypeAck   uint8 = 2
)

// encodeFrame lays out one record on disk as:
//
//	[4 bytes big-endian: length of (type byte + payload)]
//	[4 bytes big-endian: crc32(type byte + payload)]
//	[1 byte:  record type]
//	[payload]
func encodeFrame(recType uint8, payload []byte) []byte {
	body := make([]byte, 1+len(payload))
	body[0] = recType
	copy(body[1:], payload)

	frame := make([]byte, 8+len(body))
	binary.BigEndian.PutUint32(frame[0:4], uint32(len(body)))
	binary.BigEndian.PutUint32(frame[4:8], crc32.ChecksumIEEE(body))
	copy(frame[8:], body)
	return frame
}

type decodedRecord struct {
	recType uint8
	payload []byte
}

// decodeRecords reads every complete record from r. A truncated or
// checksum-mismatched final record is treated as a torn write from a crash
// mid-write and silently dropped rather than erroring: tallyd only trusts
// records whose length+crc are fully intact, which is exactly the set that
// completed an fsync before any caller was ever acked.
func decodeRecords(r io.Reader) ([]decodedRecord, error) {
	br := bufio.NewReader(r)
	var records []decodedRecord

	for {
		lenBuf := make([]byte, 4)
		if _, err := io.ReadFull(br, lenBuf); err != nil {
			break // EOF or torn tail: nothing more to safely decode.
		}
		bodyLen := binary.BigEndian.Uint32(lenBuf)

		crcBuf := make([]byte, 4)
		if _, err := io.ReadFull(br, crcBuf); err != nil {
			break
		}
		wantCRC := binary.BigEndian.Uint32(crcBuf)

		body := make([]byte, bodyLen)
		if _, err := io.ReadFull(br, body); err != nil {
			break
		}

		if crc32.ChecksumIEEE(body) != wantCRC {
			break
		}

		records = append(records, decodedRecord{recType: body[0], payload: body[1:]})
	}

	return records, nil
}
