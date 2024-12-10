package s3_dal

import "context"

type Record struct {
	Offset uint64
	Data   []byte
}

type base interface {
	Append(ctx context.Context, data []byte) (uint64, error)
	Read(ctx context.Context, offset uint64) (Record, error)
	LastRecord(ctx context.Context) (Record, error)
}
