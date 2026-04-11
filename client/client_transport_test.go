package client

import (
	"strings"
	"testing"
)

func TestResolveDialTargetAndCredentials(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		baseURL    string
		mode       TransportSecurityMode
		wantTarget string
		wantProto  string
		wantErr    bool
	}{
		{
			name:       "auto grpc uses insecure",
			baseURL:    "grpc://127.0.0.1:8080",
			mode:       TransportSecurityAuto,
			wantTarget: "127.0.0.1:8080",
			wantProto:  "insecure",
		},
		{
			name:       "auto grpcs uses tls",
			baseURL:    "grpcs://example.com:443",
			mode:       TransportSecurityAuto,
			wantTarget: "example.com:443",
			wantProto:  "tls",
		},
		{
			name:       "auto without scheme keeps backward compatible insecure",
			baseURL:    "127.0.0.1:8080",
			mode:       TransportSecurityAuto,
			wantTarget: "127.0.0.1:8080",
			wantProto:  "insecure",
		},
		{
			name:       "force tls overrides grpc scheme",
			baseURL:    "grpc://10.0.0.1:8080",
			mode:       TransportSecurityTLS,
			wantTarget: "10.0.0.1:8080",
			wantProto:  "tls",
		},
		{
			name:       "force insecure overrides grpcs scheme",
			baseURL:    "grpcs://example.com:443",
			mode:       TransportSecurityInsecure,
			wantTarget: "example.com:443",
			wantProto:  "insecure",
		},
		{
			name:       "scheme matching is case insensitive",
			baseURL:    " GRPCS://klock.ishuzhai.com:443 ",
			mode:       TransportSecurityAuto,
			wantTarget: "klock.ishuzhai.com:443",
			wantProto:  "tls",
		},
		{
			name:    "invalid mode",
			baseURL: "grpc://127.0.0.1:8080",
			mode:    "bad-mode",
			wantErr: true,
		},
		{
			name:    "empty base url",
			baseURL: " ",
			mode:    TransportSecurityAuto,
			wantErr: true,
		},
		{
			name:    "empty target after scheme",
			baseURL: "grpc://",
			mode:    TransportSecurityAuto,
			wantErr: true,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			target, creds, err := resolveDialTargetAndCredentials(tc.baseURL, tc.mode)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil; target=%q", target)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolve target/creds failed: %v", err)
			}
			if target != tc.wantTarget {
				t.Fatalf("target mismatch: want=%q got=%q", tc.wantTarget, target)
			}
			gotProto := strings.ToLower(strings.TrimSpace(creds.Info().SecurityProtocol))
			if gotProto != tc.wantProto {
				t.Fatalf("security protocol mismatch: want=%q got=%q", tc.wantProto, gotProto)
			}
		})
	}
}
