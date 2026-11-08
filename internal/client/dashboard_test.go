package client

import "testing"

func TestDashboardMetadata(t *testing.T) {
	tests := []struct {
		name                        string
		current, configured, layout string
		want                        string
	}{
		{"nothing", "", "", "", ""},
		{"configured only", "", `{"refresh_frequency":30}`, "", `{"refresh_frequency":30}`},
		{"current keys survive", `{"label_colors":{"a":"#fff"},"refresh_frequency":0}`, `{"refresh_frequency":30}`, "",
			`{"label_colors":{"a":"#fff"},"refresh_frequency":30}`},
		{"layout becomes positions", "", `{"refresh_frequency":30}`, `{"ROOT_ID":{"type":"ROOT"}}`,
			`{"positions":{"ROOT_ID":{"type":"ROOT"}},"refresh_frequency":30}`},
		{"layout alone", "", "", `{"ROOT_ID":{"type":"ROOT"}}`, `{"positions":{"ROOT_ID":{"type":"ROOT"}}}`},
		{"stale positions are replaced", `{"positions":{"old":true}}`, "", `{"new":true}`, `{"positions":{"new":true}}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := dashboardMetadata(tt.current, tt.configured, tt.layout)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("got %s, want %s", got, tt.want)
			}
		})
	}

	if _, err := dashboardMetadata("", "{", ""); err == nil {
		t.Fatal("invalid metadata must be an error")
	}
	if _, err := dashboardMetadata("", "{}", "{"); err == nil {
		t.Fatal("invalid layout must be an error")
	}
}
