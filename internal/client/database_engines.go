package client

import (
	"net/http"
	"sort"
)

// DatabaseEngine is a database engine the instance can connect to, as the available engines
// endpoint describes it: the SQLAlchemy dialect, the drivers installed for it and the shape
// of its connection URI.
type DatabaseEngine struct {
	Name                     string   `json:"name"`
	Engine                   string   `json:"engine"`
	DefaultDriver            string   `json:"default_driver"`
	AvailableDrivers         []string `json:"available_drivers"`
	SQLAlchemyURIPlaceholder string   `json:"sqlalchemy_uri_placeholder"`
	Preferred                bool     `json:"preferred"`
	Information              struct {
		DisableSSHTunneling bool `json:"disable_ssh_tunneling"`
		SupportsFileUpload  bool `json:"supports_file_upload"`
	} `json:"engine_information"`
}

// ListDatabaseEngines returns the engines the instance can connect to, the preferred ones
// first and then by name, as the Superset UI shows them.
func (c *Client) ListDatabaseEngines() ([]DatabaseEngine, error) {
	resp, err := c.DoRequest(http.MethodGet, databasesPath+"available/", nil)
	if err != nil {
		return nil, err
	}

	var body struct {
		Databases []DatabaseEngine `json:"databases"`
	}
	if err := decodeResponse(resp, &body, "list the available database engines"); err != nil {
		return nil, err
	}
	engines := body.Databases
	sort.SliceStable(engines, func(i, j int) bool {
		if engines[i].Preferred != engines[j].Preferred {
			return engines[i].Preferred
		}
		return engines[i].Name < engines[j].Name
	})
	return engines, nil
}
