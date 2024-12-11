package s3_dal

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type S3DAL struct {
	client     *s3.Client
	bucketName string
	prefix     string
	length     uint64
}

func S3DALClient(client *s3.Client, bucketName, prefix string) *S3DAL {
	return &S3DAL{
		client:     client,
		bucketName: bucketName,
		prefix:     prefix,
		length:     0,
	}
}

func (w *S3DAL) getObjectKey(offset uint64) string {
	return w.prefix + "/" + fmt.Sprintf("%020d", offset)
}

func (w *S3DAL) getOffsetFromKey(key string) (uint64, error) {
	// skip the `w.prefix` and "/"
	numStr := key[len(w.prefix)+1:]
	return strconv.ParseUint(numStr, 10, 64)
}

func crc16Fast(data []byte) uint16 {
	const polynomial uint16 = 0x1021 // CRC-16-CCITT polynomial
	crc := uint16(0xCACA)            // Common initialization value
	for _, b := range data {
		crc ^= uint16(b) << 8
		for i := 0; i < 8; i++ {
			if crc&0x8000 != 0 {
				crc = (crc << 1) ^ polynomial
			} else {
				crc <<= 1
			}
		}
	}
	return crc
}

func validateChecksum(data []byte) bool {
	if len(data) < 2 {
		return false
	}

	// Extract stored CRC (ensure correct endianness)
	storedCRC := binary.BigEndian.Uint16(data[len(data)-2:])
	// Data used for CRC calculation
	recordData := data[:len(data)-2]

	// Calculate CRC using corrected algorithm
	calculatedCRC := crc16Fast(recordData)

	// Debug logs
	fmt.Printf("Stored CRC: 0x%04X\n", storedCRC)
	fmt.Printf("Calculated CRC: 0x%04X\n", calculatedCRC)
	fmt.Printf("Data used for CRC: %v\n", recordData)

	return storedCRC == calculatedCRC
}

func prepareBody(offset uint64, data []byte) ([]byte, error) {
	// 8 bytes for offset, len(data) bytes for data, 2 bytes for CRC16
	bufferLen := 8 + len(data) + 2
	buf := bytes.NewBuffer(make([]byte, 0, bufferLen))
	if err := binary.Write(buf, binary.BigEndian, offset); err != nil {
		return nil, err
	}
	if _, err := buf.Write(data); err != nil {
		return nil, err
	}
	crc := crc16Fast(buf.Bytes()) // Exclude space for CRC during calculation
	if err := binary.Write(buf, binary.BigEndian, crc); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (w *S3DAL) Append(ctx context.Context, data []byte, fileSizeLimit uint64) (uint64, error) {
	// Check if adding the new data will exceed the allowed file size
	newDataSize := uint64(len(data))
	if w.length+newDataSize > fileSizeLimit {
		return 0, fmt.Errorf("appending data would exceed the file size limit of %d bytes", fileSizeLimit)
	}

	// Calculate the next offset
	nextOffset := w.length + 1

	// Prepare the body for upload
	buf, err := prepareBody(nextOffset, data)
	if err != nil {
		return 0, fmt.Errorf("failed to prepare object body: %w", err)
	}

	input := &s3.PutObjectInput{
		Bucket:      aws.String(w.bucketName),
		Key:         aws.String(w.getObjectKey(nextOffset)),
		Body:        bytes.NewReader(buf),
		IfNoneMatch: aws.String("*"),
	}

	// Attempt to write the data to S3
	if _, err = w.client.PutObject(ctx, input); err != nil {
		return 0, fmt.Errorf("failed to put object to S3: %w", err)
	}

	// Update the current length
	w.length = nextOffset
	return nextOffset, nil
}

func (w *S3DAL) Read(ctx context.Context, offset uint64) (Record, error) {
	key := w.getObjectKey(offset)
	input := &s3.GetObjectInput{
		Bucket: aws.String(w.bucketName),
		Key:    aws.String(key),
	}

	result, err := w.client.GetObject(ctx, input)
	if err != nil {
		return Record{}, fmt.Errorf("failed to get object from S3: %w", err)
	}
	defer result.Body.Close()

	data, err := io.ReadAll(result.Body)
	if err != nil {
		return Record{}, fmt.Errorf("failed to read object body: %w", err)
	}
	if len(data) < 10 {
		return Record{}, fmt.Errorf("invalid record: data too short")
	}

	var storedOffset uint64
	if err = binary.Read(bytes.NewReader(data[:8]), binary.BigEndian, &storedOffset); err != nil {
		return Record{}, err
	}
	if storedOffset != offset {
		return Record{}, fmt.Errorf("offset mismatch: expected %d, got %d", offset, storedOffset)
	}
	if !validateChecksum(data) {
		return Record{}, fmt.Errorf("CRC mismatch")
	}
	return Record{
		Offset: storedOffset,
		Data:   data[8 : len(data)-2],
	}, nil
}

func (w *S3DAL) LastRecord(ctx context.Context) (Record, error) {
	// Set up the input for listing objects with reversed order
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(w.bucketName),
		Prefix: aws.String(w.prefix + "/"),
	}

	// Initialize paginator
	paginator := s3.NewListObjectsV2Paginator(w.client, input)

	var lastKey string
	for paginator.HasMorePages() {
		output, err := paginator.NextPage(ctx)
		if err != nil {
			return Record{}, fmt.Errorf("failed to list objects from S3: %w", err)
		}

		// Get the last key in this page (keys are lexicographically sorted)
		if len(output.Contents) > 0 {
			lastKey = *output.Contents[len(output.Contents)-1].Key
		}
	}

	if lastKey == "" {
		return Record{}, fmt.Errorf("WAL is empty")
	}

	// Extract the offset from the last key
	maxOffset, err := w.getOffsetFromKey(lastKey)
	if err != nil {
		return Record{}, fmt.Errorf("failed to parse offset from key: %w", err)
	}

	w.length = maxOffset
	return w.Read(ctx, maxOffset)
}

/* func (w *S3DAL) LastRecord(ctx context.Context) (Record, error) {
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(w.bucketName),
		Prefix: aws.String(w.prefix + "/"),
	}
	paginator := s3.NewListObjectsV2Paginator(w.client, input)

	var maxOffset uint64 = 0
	for paginator.HasMorePages() {
		output, err := paginator.NextPage(ctx)
		if err != nil {
			return Record{}, fmt.Errorf("failed to list objects from S3: %w", err)
		}
		for _, obj := range output.Contents {
			key := *obj.Key
			offset, err := w.getOffsetFromKey(key)
			if err != nil {
				return Record{}, fmt.Errorf("failed to parse offset from key: %w", err)
			}
			if offset > maxOffset {
				maxOffset = offset
			}
		}
	}
	if maxOffset == 0 {
		return Record{}, fmt.Errorf("WAL is empty")
	}
	w.length = maxOffset
	return w.Read(ctx, maxOffset)
} */
