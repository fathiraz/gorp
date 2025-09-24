module github.com/go-gorp/gorp/v3

go 1.24

// Versions prior to 3.0.4 had a vulnerability in the dependency graph.  While we don't
// directly use yaml, I'm not comfortable encouraging people to use versions with a
// CVE - so prior versions are retracted.
//
// See CVE-2019-11254
retract [v3.0.0, v3.0.3]

require (
	github.com/go-sql-driver/mysql v1.8.1
	github.com/jackc/pgx/v5 v5.5.1
	github.com/jmoiron/sqlx v1.4.0
	github.com/lib/pq v1.10.9
	github.com/mattn/go-sqlite3 v1.14.22
	github.com/microsoft/go-mssqldb v1.6.0
	github.com/poy/onpar v0.3.2
	github.com/stretchr/testify v1.11.1
	go.opentelemetry.io/otel v1.21.0
	go.opentelemetry.io/otel/trace v1.21.0
)

require (
	filippo.io/edwards25519 v1.1.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/go-logr/logr v1.3.0 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/golang-sql/civil v0.0.0-20220223132316-b832511892a9 // indirect
	github.com/golang-sql/sqlexp v0.1.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20221227161230-091c0ba34f0a // indirect
	github.com/jackc/puddle/v2 v2.2.1 // indirect
	github.com/kr/pretty v0.3.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/stretchr/objx v0.5.2 // indirect
	go.opentelemetry.io/otel/metric v1.21.0 // indirect
	golang.org/x/crypto v0.12.0 // indirect
	golang.org/x/sync v0.1.0 // indirect
	golang.org/x/text v0.12.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
