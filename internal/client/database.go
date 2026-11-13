package client

import (
	"errors"
	"fmt"
	"net/http"
)

const databasesPath = "/api/v1/database/"

// DatabaseConnection is a database connection as the API manages it. In the values Superset
// returns, the password of the URI and the secrets of the SSH tunnel are masked.
type DatabaseConnection struct {
	ID                   int64
	Name                 string
	SQLAlchemyURI        string
	Backend              string
	Parameters           ConnectionParameters
	ExposeInSQLLab       bool
	AllowCTAS            bool
	AllowCVAS            bool
	AllowDML             bool
	AllowRunAsync        bool
	AllowFileUpload      bool
	ImpersonateUser      bool
	CacheTimeout         *int64
	ForceCTASSchema      string
	Extra                string
	MaskedEncryptedExtra string
	ServerCert           string
	SSHTunnel            *SSHTunnel
}

// ConnectionParameters are the parts of the URI that Superset reports for engines it can parse;
// they are empty for the other engines.
type ConnectionParameters struct {
	Host     string
	Port     int64
	Username string
	Database string
}

// SSHTunnel is the tunnel through which Superset reaches the database. Superset authenticates
// with the password or with the private key.
type SSHTunnel struct {
	ServerAddress      string
	ServerPort         int64
	Username           string
	Password           string
	PrivateKey         string
	PrivateKeyPassword string
}

// databasePayload is the body of the create and update requests. Nullable fields are sent as
// null when empty, so an update can clear them; a null tunnel removes the tunnel on recent
// versions of Superset.
type databasePayload struct {
	Name                 string            `json:"database_name"`
	SQLAlchemyURI        string            `json:"sqlalchemy_uri"`
	ExposeInSQLLab       bool              `json:"expose_in_sqllab"`
	AllowCTAS            bool              `json:"allow_ctas"`
	AllowCVAS            bool              `json:"allow_cvas"`
	AllowDML             bool              `json:"allow_dml"`
	AllowRunAsync        bool              `json:"allow_run_async"`
	AllowFileUpload      bool              `json:"allow_file_upload"`
	ImpersonateUser      bool              `json:"impersonate_user"`
	CacheTimeout         *int64            `json:"cache_timeout"`
	ForceCTASSchema      *string           `json:"force_ctas_schema"`
	Extra                string            `json:"extra"`
	MaskedEncryptedExtra *string           `json:"masked_encrypted_extra"`
	ServerCert           *string           `json:"server_cert"`
	SSHTunnel            *sshTunnelPayload `json:"ssh_tunnel"`
}

type sshTunnelPayload struct {
	ServerAddress      string `json:"server_address"`
	ServerPort         int64  `json:"server_port"`
	Username           string `json:"username"`
	Password           string `json:"password,omitempty"`
	PrivateKey         string `json:"private_key,omitempty"`
	PrivateKeyPassword string `json:"private_key_password,omitempty"`
}

func newDatabasePayload(conn DatabaseConnection) databasePayload {
	payload := databasePayload{
		Name:                 conn.Name,
		SQLAlchemyURI:        conn.SQLAlchemyURI,
		ExposeInSQLLab:       conn.ExposeInSQLLab,
		AllowCTAS:            conn.AllowCTAS,
		AllowCVAS:            conn.AllowCVAS,
		AllowDML:             conn.AllowDML,
		AllowRunAsync:        conn.AllowRunAsync,
		AllowFileUpload:      conn.AllowFileUpload,
		ImpersonateUser:      conn.ImpersonateUser,
		CacheTimeout:         conn.CacheTimeout,
		ForceCTASSchema:      nullIfEmpty(conn.ForceCTASSchema),
		Extra:                conn.Extra,
		MaskedEncryptedExtra: nullIfEmpty(conn.MaskedEncryptedExtra),
		ServerCert:           nullIfEmpty(conn.ServerCert),
	}
	if tunnel := conn.SSHTunnel; tunnel != nil {
		payload.SSHTunnel = &sshTunnelPayload{
			ServerAddress:      tunnel.ServerAddress,
			ServerPort:         tunnel.ServerPort,
			Username:           tunnel.Username,
			Password:           tunnel.Password,
			PrivateKey:         tunnel.PrivateKey,
			PrivateKeyPassword: tunnel.PrivateKeyPassword,
		}
	}
	return payload
}

// databaseResult is the connection as the connection endpoint returns it. For a database that
// does not exist the endpoint answers with an empty result instead of a 404, so the id tells
// whether the database was found.
type databaseResult struct {
	ID            int64  `json:"id"`
	Name          string `json:"database_name"`
	SQLAlchemyURI string `json:"sqlalchemy_uri"`
	Backend       string `json:"backend"`
	Parameters    struct {
		Host     string   `json:"host"`
		Port     looseInt `json:"port"`
		Username string   `json:"username"`
		Database string   `json:"database"`
	} `json:"parameters"`
	ExposeInSQLLab       bool     `json:"expose_in_sqllab"`
	AllowCTAS            bool     `json:"allow_ctas"`
	AllowCVAS            bool     `json:"allow_cvas"`
	AllowDML             bool     `json:"allow_dml"`
	AllowRunAsync        bool     `json:"allow_run_async"`
	AllowFileUpload      bool     `json:"allow_file_upload"`
	ImpersonateUser      bool     `json:"impersonate_user"`
	CacheTimeout         looseInt `json:"cache_timeout"`
	ForceCTASSchema      string   `json:"force_ctas_schema"`
	Extra                string   `json:"extra"`
	MaskedEncryptedExtra string   `json:"masked_encrypted_extra"`
	ServerCert           string   `json:"server_cert"`
	SSHTunnel            *struct {
		ServerAddress string   `json:"server_address"`
		ServerPort    looseInt `json:"server_port"`
		Username      string   `json:"username"`
	} `json:"ssh_tunnel"`
}

func databasePath(id int64) string {
	return fmt.Sprintf("%s%d", databasesPath, id)
}

// CreateDatabaseConnection creates a database connection and returns its id. Superset tests the
// connection before creating it.
func (c *Client) CreateDatabaseConnection(conn DatabaseConnection) (int64, error) {
	defer c.forgetDatabases()
	headers, cookies, err := c.csrfHeaders()
	if err != nil {
		return 0, err
	}

	resp, err := c.DoRequestWithHeadersAndCookies(http.MethodPost, databasesPath, newDatabasePayload(conn), headers, cookies)
	if err != nil {
		return 0, err
	}

	var created struct {
		ID int64 `json:"id"`
	}
	if err := decodeResponse(resp, &created, "create database connection"); err != nil {
		return 0, err
	}
	return created.ID, nil
}

// GetDatabaseConnection returns the database connection with the given id, or ErrNotFound.
func (c *Client) GetDatabaseConnection(id int64) (*DatabaseConnection, error) {
	resp, err := c.DoRequest(http.MethodGet, databasePath(id)+"/connection", nil)
	if err != nil {
		return nil, err
	}

	var body struct {
		Result databaseResult `json:"result"`
	}
	if err := decodeResponse(resp, &body, "fetch database connection"); err != nil {
		return nil, err
	}
	if body.Result.ID == 0 {
		return nil, fmt.Errorf("failed to fetch database connection: %w", ErrNotFound)
	}

	result := body.Result
	conn := &DatabaseConnection{
		ID:            id,
		Name:          result.Name,
		SQLAlchemyURI: result.SQLAlchemyURI,
		Backend:       result.Backend,
		Parameters: ConnectionParameters{
			Host:     result.Parameters.Host,
			Port:     result.Parameters.Port.int64(),
			Username: result.Parameters.Username,
			Database: result.Parameters.Database,
		},
		ExposeInSQLLab:       result.ExposeInSQLLab,
		AllowCTAS:            result.AllowCTAS,
		AllowCVAS:            result.AllowCVAS,
		AllowDML:             result.AllowDML,
		AllowRunAsync:        result.AllowRunAsync,
		AllowFileUpload:      result.AllowFileUpload,
		ImpersonateUser:      result.ImpersonateUser,
		CacheTimeout:         result.CacheTimeout.value,
		ForceCTASSchema:      result.ForceCTASSchema,
		Extra:                result.Extra,
		MaskedEncryptedExtra: result.MaskedEncryptedExtra,
		ServerCert:           result.ServerCert,
	}
	if tunnel := result.SSHTunnel; tunnel != nil {
		conn.SSHTunnel = &SSHTunnel{
			ServerAddress: tunnel.ServerAddress,
			ServerPort:    tunnel.ServerPort.int64(),
			Username:      tunnel.Username,
		}
	}
	return conn, nil
}

// UpdateDatabaseConnection replaces the attributes of a database connection. When removeTunnel
// is set, the SSH tunnel of the connection is removed: recent versions of Superset do that for a
// null tunnel in the update, Superset 3.1 only through its own endpoint, so both are used.
func (c *Client) UpdateDatabaseConnection(id int64, conn DatabaseConnection, removeTunnel bool) error {
	defer c.forgetDatabases()
	headers, cookies, err := c.csrfHeaders()
	if err != nil {
		return err
	}

	resp, err := c.DoRequestWithHeadersAndCookies(http.MethodPut, databasePath(id), newDatabasePayload(conn), headers, cookies)
	if err != nil {
		return err
	}
	if err := decodeResponse(resp, nil, "update database connection"); err != nil {
		return err
	}
	if !removeTunnel {
		return nil
	}

	resp, err = c.DoRequestWithHeadersAndCookies(http.MethodDelete, databasePath(id)+"/ssh_tunnel/", nil, headers, cookies)
	if err != nil {
		return err
	}
	if err := decodeResponse(resp, nil, "remove the SSH tunnel of the database connection"); err != nil && !errors.Is(err, ErrNotFound) {
		return err
	}
	return nil
}

// DeleteDatabaseConnection deletes a database connection; a connection that no longer exists is
// reported as ErrNotFound.
func (c *Client) DeleteDatabaseConnection(id int64) error {
	defer c.forgetDatabases()
	headers, cookies, err := c.csrfHeaders()
	if err != nil {
		return err
	}

	resp, err := c.DoRequestWithHeadersAndCookies(http.MethodDelete, databasePath(id), nil, headers, cookies)
	if err != nil {
		return err
	}
	return decodeResponse(resp, nil, "delete database connection")
}

// int64 returns the value, or zero when there is none.
func (l looseInt) int64() int64 {
	if l.value == nil {
		return 0
	}
	return *l.value
}
