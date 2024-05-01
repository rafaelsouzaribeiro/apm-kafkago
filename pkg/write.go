package pkg

import (
	"context"

	"github.com/segmentio/kafka-go"
	"go.elastic.co/apm/v2"
)

const (
	transactionProducerType = "kafka-producer"
	spanWriteMessageType    = "WriteMessage"
)

type Writer struct {
	W *kafka.Writer
}

func WrapWriter(w *kafka.Writer) *Writer {
	return &Writer{
		W: w,
	}
}

func (w *Writer) WriteMessages(ctx context.Context, msgs ...kafka.Message) error {
	tx := apm.TransactionFromContext(ctx)
	if tx == nil {
		tx = apm.DefaultTracer().StartTransaction("Produce "+w.W.Topic, transactionProducerType)
		defer tx.End()
	}
	ctx = apm.ContextWithTransaction(ctx, tx)
	for i := range msgs {
		span, _ := apm.StartSpan(ctx, "Produce "+string(msgs[i].Key), spanWriteMessageType)
		span.Context.SetLabel("topic", w.W.Topic)
		span.Context.SetLabel("key", string(msgs[i].Key))
		span.End()
	}
	err := w.W.WriteMessages(ctx, msgs...)
	if err != nil {
		apm.CaptureError(ctx, err).Send()
	}
	return err
}
