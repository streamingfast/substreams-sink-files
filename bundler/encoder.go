package bundler

import (
	"encoding/json"
	"fmt"
	"github.com/golang/protobuf/proto"
)

type Encoder func([]proto.Message) ([]byte, error)

func JSONEncode(messages []proto.Message) ([]byte, error) {
	buf := []byte{}
	for i := 0; i < len(messages); i++ {
		data, err := json.Marshal(messages[i])
		if err != nil {
			return nil, fmt.Errorf("json marshal: %w", err)
		}
		buf = append(buf, data...)
		if i < len(messages)-1 {
			buf = append(buf, byte('\n'))
		}
	}
	return buf, nil
}
