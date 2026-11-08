# Local Superset instances for testing the provider

The compose file starts two Superset instances and a PostgreSQL to test the provider against,
the same setup the resources of this provider were verified on:

| Service      | Image                  | URL                     | Account       |
|--------------|------------------------|-------------------------|---------------|
| `superset-6` | `apache/superset:6.1.0` | http://localhost:8088   | admin / admin |
| `superset-3` | `apache/superset:3.1.3` | http://localhost:8089   | admin / admin |
| `postgres`   | `postgres:16`           | `tf-review-pg:5432` (inside the network) | pguser / pgpass, database `pgdb` |

The PostgreSQL holds the table `public.tf_review_sales` (`id`, `amount`, `day`) with two rows,
enough to build datasets, charts, dashboards, reports and alerts on. Both Superset instances
enable the feature flags the provider documents (`EMBEDDED_SUPERSET`, `SSH_TUNNELING`,
`ENABLE_TEMPLATE_PROCESSING`, `ALERT_REPORTS`, and `TAGGING_SYSTEM` on 6.1.0 only) and the
Flask-AppBuilder security API. Each instance keeps its metadata in a volume, so it survives
`docker compose down` without `-v`.

## Usage

```shell
cd docker-compose
docker compose up -d
./setup.sh   # registers the PostgreSQL as the connection "tf-review pg" in both instances
```

The first start takes a few minutes: the instances upgrade their metadata database, create the
admin account and, on 6.1.0, install the PostgreSQL driver. `docker compose ps` shows them
`healthy` when they are ready.

Then point the provider at one of them:

```shell
export SUPERSET_HOST=http://localhost:8088   # or 8089 for 3.1.3
export SUPERSET_USERNAME=admin
export SUPERSET_PASSWORD=admin
```

To run a locally built provider instead of the one from the registry, use a `dev_overrides`
block in a CLI configuration file:

```hcl
provider_installation {
  dev_overrides {
    "platacard/superset" = "/path/to/the/directory/holding/terraform-provider-superset"
  }
  direct {}
}
```

```shell
go build -o /path/to/the/directory/holding/terraform-provider-superset .
TF_CLI_CONFIG_FILE=/path/to/that/file terraform plan
```

## Notes

* The reports and alerts can be created, but nothing is sent: the instances run no workers.
* Superset 3.1 with `TAGGING_SYSTEM` on cannot delete a chart or a dashboard whenever another
  object has the same numeric id, so the flag stays off on 3.1.3; turn it on in
  `superset-3.1/superset_config.py` only to try tags there.
* `docker compose down -v` removes the metadata and the PostgreSQL data; the next start begins
  from scratch and `setup.sh` has to run again.
