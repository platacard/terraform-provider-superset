# Testing Guide

This document describes how to run tests for the Terraform Superset Provider.

## Unit Tests

Unit tests don't require a running Superset instance. They use mocked HTTP responses.

```shell
# Run all unit tests
go test ./... -v

# Run only client tests (OIDC, caching, etc.)
go test ./internal/client/... -v

# Run only provider tests
go test ./internal/provider/... -v

# Run a specific test
go test ./internal/client/... -v -run TestRefreshOIDCTokenIfNeeded
```

## Acceptance Tests

Acceptance tests require a running Superset instance and create real resources.

### Using Database Authentication

```shell
export TF_ACC=1
export SUPERSET_HOST="http://localhost:8088"
export SUPERSET_USERNAME="admin"
export SUPERSET_PASSWORD="admin"

go test ./internal/provider/... -v -timeout 120m
```

Or as a one-liner:

```shell
TF_ACC=1 SUPERSET_HOST="http://localhost:8088" SUPERSET_USERNAME="admin" SUPERSET_PASSWORD="admin" go test ./internal/provider/... -v -timeout 120m
```

### Using OIDC Authentication

For Superset instances configured with OAuth/OIDC (e.g., Keycloak):

```shell
export TF_ACC=1
export SUPERSET_HOST="http://localhost:8088"
export SUPERSET_OIDC_TOKEN_URL="http://localhost:8100/realms/myrealm/protocol/openid-connect/token"
export SUPERSET_CLIENT_ID="my-client-id"
export SUPERSET_CLIENT_SECRET="my-client-secret"

go test ./internal/provider/... -v -timeout 120m
```

### Using Make

The Makefile provides a shortcut:

```shell
# Set environment variables first, then:
make testacc

# Or with test arguments:
make testacc TESTARGS="-run TestAccChartResource"
```

## Running Specific Test Suites

```shell
# Chart resource tests only
TF_ACC=1 go test ./internal/provider/... -v -run TestAccChartResource

# Dashboard resource tests only
TF_ACC=1 go test ./internal/provider/... -v -run TestAccDashboardResource

# All data source tests
TF_ACC=1 go test ./internal/provider/... -v -run "DataSource"
```

## Test Coverage

Generate a coverage report:

```shell
go test ./... -coverprofile=coverage.out
go tool cover -html=coverage.out -o coverage.html
```

## Troubleshooting

### Tests Skip Immediately

If acceptance tests skip with "Acceptance tests skipped unless env 'TF_ACC' set", ensure you've set:

```shell
export TF_ACC=1
```

### Authentication Failures

Verify your Superset credentials work by testing the API directly:

```shell
curl -X POST "${SUPERSET_HOST}/api/v1/security/login" \
  -H "Content-Type: application/json" \
  -d '{"username": "admin", "password": "admin", "provider": "db"}'
```

### OIDC Token Issues

For OIDC authentication, verify your token endpoint:

```shell
curl -X POST "${SUPERSET_OIDC_TOKEN_URL}" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=client_credentials&client_id=${SUPERSET_CLIENT_ID}&client_secret=${SUPERSET_CLIENT_SECRET}"
```

