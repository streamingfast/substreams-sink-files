package protox

import (
	"time"

	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func DynamicAsTimestamppb(message protoreflect.Message) *timestamppb.Timestamp {
	seconds, nanos := DynamicAsTimestampParts(message)

	return &timestamppb.Timestamp{
		Seconds: seconds,
		Nanos:   int32(nanos),
	}
}

func DynamicAsTimestampTime(message protoreflect.Message) (out time.Time) {
	seconds, nanos := DynamicAsTimestampParts(message)
	return time.Unix(seconds, nanos).UTC()
}

func DynamicAsTimestampParts(message protoreflect.Message) (seconds, nanos int64) {
	if message == nil || message.Descriptor().FullName() != "google.protobuf.Timestamp" {
		return
	}

	// FIXME: What is the most efficient way to extract the seconds and nanos fields from a dynamic message?
	// I tried caching the field descriptor with:
	//
	// var timestampSecondsField = (&timestamppb.Timestamp{}).ProtoReflect().Descriptor().Fields().ByName(protoreflect.Name("seconds"))
	// var timestampNanosField = (&timestamppb.Timestamp{}).ProtoReflect().Descriptor().Fields().ByName(protoreflect.Name("nanos"))
	//
	// But this led to error such as `panic: proto: google.protobuf.Timestamp.seconds: field descriptor does not belong to this message`
	// which seems to imply there is multiple `google.protobuf.Timestamp` instance, which is quite possible knowing
	// Substreams system.
	//
	// For now, workaround that works in all cases is to iterate over the fields and find the one we are looking for.

	var foundSeconds, foundNanos bool
	message.Range(func(f protoreflect.FieldDescriptor, value protoreflect.Value) bool {
		switch f.FullName() {
		case "google.protobuf.Timestamp.seconds":
			seconds = value.Int()
			foundSeconds = true
		case "google.protobuf.Timestamp.nanos":
			nanos = value.Int()
			foundNanos = true
		}

		return !foundSeconds || !foundNanos
	})

	return
}
