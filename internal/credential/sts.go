// Package credential 定义运行期凭证轮转的内存协议。它不负责获取或持久化凭证。
package credential

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

const (
	MaxRotationPayloadBytes = 64 << 10
	RotationXattr           = "user.agent-memory-fuse.rotate-credential"
)

// STS 是必须一起轮转的完整临时凭证，避免请求混用不同代的 AK/SK/token。
type STS struct {
	AccessKeyID     string `json:"accessKeyId"`
	AccessKeySecret string `json:"accessKeySecret"`
	Token           string `json:"stsToken"`
}

func (c STS) Validate() error {
	if err := c.validateAccessKey(); err != nil {
		return err
	}
	if c.Token == "" {
		return errors.New("stsToken is required")
	}
	return nil
}

func (c STS) validateAccessKey() error {
	switch {
	case c.AccessKeyID == "":
		return errors.New("accessKeyId is required")
	case c.AccessKeySecret == "":
		return errors.New("accessKeySecret is required")
	default:
		return nil
	}
}

func DecodeSTS(r io.Reader) (STS, error) {
	payload, err := io.ReadAll(io.LimitReader(r, MaxRotationPayloadBytes+1))
	if err != nil {
		return STS{}, fmt.Errorf("read STS credential: %w", err)
	}
	return ParseSTS(payload)
}

func ParseSTS(payload []byte) (STS, error) {
	credentials, err := parseAccessKeyCredentials(payload)
	if err != nil {
		return STS{}, err
	}
	if err := credentials.Validate(); err != nil {
		return STS{}, err
	}
	return credentials, nil
}

// ParseAccessKeyCredentials parses mount credentials. A missing or empty
// stsToken selects long-lived AK/SK mode; rotation continues to use ParseSTS
// and therefore requires a non-empty token.
func ParseAccessKeyCredentials(payload []byte) (STS, error) {
	credentials, err := parseAccessKeyCredentials(payload)
	if err != nil {
		return STS{}, err
	}
	if err := credentials.validateAccessKey(); err != nil {
		return STS{}, err
	}
	return credentials, nil
}

func parseAccessKeyCredentials(payload []byte) (STS, error) {
	if len(payload) > MaxRotationPayloadBytes {
		return STS{}, errors.New("STS credential payload exceeds 64 KiB")
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	opening, err := decoder.Token()
	if err != nil || opening != json.Delim('{') {
		return STS{}, errors.New("decode STS credential JSON")
	}
	var credentials STS
	seen := make(map[string]struct{}, 3)
	for decoder.More() {
		nameToken, err := decoder.Token()
		if err != nil {
			return STS{}, errors.New("decode STS credential JSON")
		}
		name, ok := nameToken.(string)
		if !ok {
			return STS{}, errors.New("decode STS credential JSON")
		}
		if _, duplicate := seen[name]; duplicate {
			return STS{}, errors.New("STS credential contains a duplicate field")
		}
		seen[name] = struct{}{}
		var destination *string
		switch name {
		case "accessKeyId":
			destination = &credentials.AccessKeyID
		case "accessKeySecret":
			destination = &credentials.AccessKeySecret
		case "stsToken":
			destination = &credentials.Token
		default:
			return STS{}, errors.New("STS credential contains an unknown field")
		}
		if err := decoder.Decode(destination); err != nil {
			return STS{}, errors.New("decode STS credential JSON")
		}
	}
	closing, err := decoder.Token()
	if err != nil || closing != json.Delim('}') {
		return STS{}, errors.New("decode STS credential JSON")
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return STS{}, errors.New("STS credential must contain exactly one JSON value")
	}
	return credentials, nil
}

func EncodeSTS(credentials STS) ([]byte, error) {
	if err := credentials.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(credentials)
}
