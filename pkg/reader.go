package pkg

import (
	"context"

	"github.com/segmentio/kafka-go"
	"go.elastic.co/apm/v2"
)

const (
	transactionConsumerType = "kafka-consumer"
	spanReadMessageType     = "ReadMessage"
)

type Reader struct {
	R *kafka.Reader
}

func WrapReader(r *kafka.Reader) *Reader {
	return &Reader{
		R: r,
	}
}

func (r *Reader) ReadMessage(ctx context.Context) (kafka.Message, error) {
	msg, err := r.R.ReadMessage(ctx)
	if err != nil {
		return msg, err
	}

	transaction := apm.DefaultTracer().StartTransaction("Consume "+r.R.Config().Topic+" "+string(msg.Key), transactionConsumerType)
	ctx = apm.ContextWithTransaction(ctx, transaction)
	span, _ := apm.StartSpan(ctx, "Consume "+string(msg.Key), spanReadMessageType)
	span.Context.SetLabel("topic", r.R.Config().Topic)
	span.Context.SetLabel("key", string(msg.Key))
	span.Context.SetLabel("consumer-group", string(r.R.Config().GroupID))
	defer span.End()
	defer transaction.End()
	return msg, nil

}
