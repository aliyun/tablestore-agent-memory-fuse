package credential

import (
	"strings"
	"testing"
)

func TestDecodeSTS(t *testing.T) {
	got, err := DecodeSTS(strings.NewReader(`{
		"accessKeyId":"new-ak",
		"accessKeySecret":"new-sk",
		"stsToken":"new-token"
	}`))
	if err != nil {
		t.Fatal(err)
	}
	want := STS{AccessKeyID: "new-ak", AccessKeySecret: "new-sk", Token: "new-token"}
	if got != want {
		t.Fatalf("credentials=%+v want %+v", got, want)
	}
	payload, err := EncodeSTS(got)
	if err != nil {
		t.Fatal(err)
	}
	if string(payload) != `{"accessKeyId":"new-ak","accessKeySecret":"new-sk","stsToken":"new-token"}` {
		t.Fatalf("payload=%q", payload)
	}
}

func TestParseAccessKeyCredentialsAllowsMissingOrEmptySTSToken(t *testing.T) {
	for _, input := range []string{
		`{"accessKeyId":"ak","accessKeySecret":"sk"}`,
		`{"accessKeyId":"ak","accessKeySecret":"sk","stsToken":""}`,
	} {
		got, err := ParseAccessKeyCredentials([]byte(input))
		if err != nil {
			t.Fatal(err)
		}
		if got.AccessKeyID != "ak" || got.AccessKeySecret != "sk" || got.Token != "" {
			t.Fatalf("credentials=%+v", got)
		}
	}
}

func TestDecodeSTSRejectsInvalidInput(t *testing.T) {
	tests := map[string]string{
		"missing token":  `{"accessKeyId":"ak","accessKeySecret":"sk"}`,
		"empty key":      `{"accessKeyId":"","accessKeySecret":"sk","stsToken":"token"}`,
		"unknown field":  `{"accessKeyId":"ak","accessKeySecret":"sk","stsToken":"token","extra":true}`,
		"case alias":     `{"ACCESSKEYID":"ak","accessKeySecret":"sk","stsToken":"token"}`,
		"duplicate key":  `{"accessKeyId":"ak","accessKeySecret":"sk","stsToken":"one","stsToken":"two"}`,
		"second value":   `{"accessKeyId":"ak","accessKeySecret":"sk","stsToken":"token"}{}`,
		"trailing bytes": `{"accessKeyId":"ak","accessKeySecret":"sk","stsToken":"token"} x`,
	}
	for name, input := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeSTS(strings.NewReader(input)); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestDecodeSTSRejectsOversizedInputWithoutLeakingIt(t *testing.T) {
	secret := strings.Repeat("s", MaxRotationPayloadBytes)
	_, err := DecodeSTS(strings.NewReader(`{"accessKeyId":"ak","accessKeySecret":"` + secret + `","stsToken":"token"}`))
	if err == nil {
		t.Fatal("expected oversized-input error")
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatal("error leaked credential input")
	}
}

func TestRotationXattrUsesAgentMemoryFuseNamespace(t *testing.T) {
	const want = "user.agent-memory-fuse.rotate-credential"
	if RotationXattr != want {
		t.Fatalf("RotationXattr=%q want %q", RotationXattr, want)
	}
}
