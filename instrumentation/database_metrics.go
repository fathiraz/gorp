package instrumentation

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jmoiron/sqlx"
)

// DatabaseSpecificCollector provides database-specific metrics collection
type DatabaseSpecificCollector struct {
	collector     MetricsCollector
	driverName    string
	db            interface{} // Can be *sqlx.DB, *pgxpool.Pool, etc.
	queryInterval time.Duration

	// Collectors for different databases
	postgresql *PostgreSQLMetricsCollector
	mysql      *MySQLMetricsCollector
	sqlite     *SQLiteMetricsCollector
	sqlserver  *SQLServerMetricsCollector

	// Context for background collection
	ctx    context.Context
	cancel context.CancelFunc
}

// NewDatabaseSpecificCollector creates a database-specific metrics collector
func NewDatabaseSpecificCollector(collector MetricsCollector, driverName string, db interface{}, queryInterval time.Duration) *DatabaseSpecificCollector {
	ctx, cancel := context.WithCancel(context.Background())

	dsc := &DatabaseSpecificCollector{
		collector:     collector,
		driverName:    strings.ToLower(driverName),
		db:            db,
		queryInterval: queryInterval,
		ctx:           ctx,
		cancel:        cancel,
	}

	// Initialize database-specific collector
	switch dsc.driverName {
	case "postgres", "postgresql", "pgx":
		dsc.postgresql = NewPostgreSQLMetricsCollector(collector, db)
	case "mysql":
		dsc.mysql = NewMySQLMetricsCollector(collector, db)
	case "sqlite3", "sqlite":
		dsc.sqlite = NewSQLiteMetricsCollector(collector, db)
	case "sqlserver", "mssql":
		dsc.sqlserver = NewSQLServerMetricsCollector(collector, db)
	}

	// Start background collection
	go dsc.backgroundCollection()

	return dsc
}

// Close stops the background collection
func (dsc *DatabaseSpecificCollector) Close() {
	dsc.cancel()
}

// backgroundCollection runs periodic database-specific metrics collection
func (dsc *DatabaseSpecificCollector) backgroundCollection() {
	ticker := time.NewTicker(dsc.queryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-dsc.ctx.Done():
			return
		case <-ticker.C:
			dsc.collectMetrics()
		}
	}
}

// collectMetrics collects database-specific metrics
func (dsc *DatabaseSpecificCollector) collectMetrics() {
	switch dsc.driverName {
	case "postgres", "postgresql", "pgx":
		if dsc.postgresql != nil {
			dsc.postgresql.CollectMetrics(dsc.ctx)
		}
	case "mysql":
		if dsc.mysql != nil {
			dsc.mysql.CollectMetrics(dsc.ctx)
		}
	case "sqlite3", "sqlite":
		if dsc.sqlite != nil {
			dsc.sqlite.CollectMetrics(dsc.ctx)
		}
	case "sqlserver", "mssql":
		if dsc.sqlserver != nil {
			dsc.sqlserver.CollectMetrics(dsc.ctx)
		}
	}
}

// PostgreSQLMetricsCollector collects PostgreSQL-specific metrics
type PostgreSQLMetricsCollector struct {
	collector MetricsCollector
	db        interface{}
}

// NewPostgreSQLMetricsCollector creates a PostgreSQL metrics collector
func NewPostgreSQLMetricsCollector(collector MetricsCollector, db interface{}) *PostgreSQLMetricsCollector {
	return &PostgreSQLMetricsCollector{
		collector: collector,
		db:        db,
	}
}

// CollectMetrics collects PostgreSQL-specific metrics
func (pmc *PostgreSQLMetricsCollector) CollectMetrics(ctx context.Context) {
	pmc.collectConnectionStats(ctx)
	pmc.collectReplicationStats(ctx)
	pmc.collectLockStats(ctx)
	pmc.collectTableStats(ctx)
	pmc.collectIndexStats(ctx)
	pmc.collectBackgroundWriterStats(ctx)
	pmc.collectVacuumStats(ctx)
}

// collectConnectionStats collects PostgreSQL connection statistics
func (pmc *PostgreSQLMetricsCollector) collectConnectionStats(ctx context.Context) {
	query := `
		SELECT state, datname, count(*) as count
		FROM pg_stat_activity
		WHERE pid <> pg_backend_pid()
		GROUP BY state, datname
	`

	rows, err := pmc.queryRows(ctx, query)
	if err != nil {
		return
	}

	for _, row := range rows {
		state := pmc.getString(row, "state")
		database := pmc.getString(row, "datname")
		count := pmc.getFloat64(row, "count")

		if pc, ok := pmc.collector.(*PrometheusCollector); ok {
			pc.RecordPostgreSQLConnections(state, database, int(count))
		} else {
			labels := map[string]string{"state": state, "database": database}
			pmc.collector.Gauge("postgresql_connections_by_state", count, labels)
		}
	}
}

// collectReplicationStats collects PostgreSQL replication statistics
func (pmc *PostgreSQLMetricsCollector) collectReplicationStats(ctx context.Context) {
	query := `
		SELECT
			client_addr,
			state,
			EXTRACT(EPOCH FROM (now() - backend_start)) as seconds_since_start,
			CASE
				WHEN pg_is_in_recovery() THEN EXTRACT(EPOCH FROM (now() - pg_last_xact_replay_timestamp()))
				ELSE EXTRACT(EPOCH FROM (now() - backend_start))
			END as lag_seconds
		FROM pg_stat_replication
	`

	rows, err := pmc.queryRows(ctx, query)
	if err != nil {
		return
	}

	for _, row := range rows {
		clientAddr := pmc.getString(row, "client_addr")
		state := pmc.getString(row, "state")
		lagSeconds := pmc.getFloat64(row, "lag_seconds")

		if pc, ok := pmc.collector.(*PrometheusCollector); ok {
			pc.RecordPostgreSQLReplicationLag(lagSeconds)
		} else {
			labels := map[string]string{"client": clientAddr, "state": state}
			pmc.collector.Gauge("postgresql_replication_lag_seconds", lagSeconds, labels)
		}
	}
}

// collectLockStats collects PostgreSQL lock statistics
func (pmc *PostgreSQLMetricsCollector) collectLockStats(ctx context.Context) {
	query := `
		SELECT locktype, mode, count(*) as count
		FROM pg_locks
		WHERE NOT granted
		GROUP BY locktype, mode
	`

	rows, err := pmc.queryRows(ctx, query)
	if err != nil {
		return
	}

	for _, row := range rows {
		lockType := pmc.getString(row, "locktype")
		mode := pmc.getString(row, "mode")
		count := pmc.getFloat64(row, "count")

		if pc, ok := pmc.collector.(*PrometheusCollector); ok {
			pc.RecordPostgreSQLLocks(lockType, mode, int(count))
		} else {
			labels := map[string]string{"lock_type": lockType, "mode": mode}
			pmc.collector.Gauge("postgresql_locks_held", count, labels)
		}
	}
}

// collectTableStats collects PostgreSQL table statistics
func (pmc *PostgreSQLMetricsCollector) collectTableStats(ctx context.Context) {
	query := `
		SELECT
			schemaname,
			tablename,
			pg_total_relation_size(schemaname||'.'||tablename) as size_bytes,
			seq_scan,
			seq_tup_read,
			idx_scan,
			idx_tup_fetch,
			n_tup_ins,
			n_tup_upd,
			n_tup_del
		FROM pg_stat_user_tables
		ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
		LIMIT 20
	`

	rows, err := pmc.queryRows(ctx, query)
	if err != nil {
		return
	}

	for _, row := range rows {
		schema := pmc.getString(row, "schemaname")
		table := pmc.getString(row, "tablename")
		sizeBytes := pmc.getFloat64(row, "size_bytes")
		seqScan := pmc.getFloat64(row, "seq_scan")
		idxScan := pmc.getFloat64(row, "idx_scan")

		labels := map[string]string{"schema": schema, "table": table}
		pmc.collector.Gauge("postgresql_table_size_bytes", sizeBytes, labels)
		pmc.collector.Counter("postgresql_seq_scans_total", map[string]string{"schema": schema, "table": table})
		pmc.collector.Counter("postgresql_idx_scans_total", map[string]string{"schema": schema, "table": table})

		// Calculate index usage ratio
		if seqScan+idxScan > 0 {
			idxRatio := idxScan / (seqScan + idxScan)
			pmc.collector.Gauge("postgresql_index_usage_ratio", idxRatio, labels)
		}
	}
}

// collectIndexStats collects PostgreSQL index statistics
func (pmc *PostgreSQLMetricsCollector) collectIndexStats(ctx context.Context) {
	query := `
		SELECT
			schemaname,
			tablename,
			indexname,
			idx_scan,
			idx_tup_read,
			idx_tup_fetch,
			pg_relation_size(indexrelid) as size_bytes
		FROM pg_stat_user_indexes
		WHERE idx_scan = 0
		ORDER BY pg_relation_size(indexrelid) DESC
		LIMIT 20
	`

	rows, err := pmc.queryRows(ctx, query)
	if err != nil {
		return
	}

	for _, row := range rows {
		schema := pmc.getString(row, "schemaname")
		table := pmc.getString(row, "tablename")
		index := pmc.getString(row, "indexname")
		sizeBytes := pmc.getFloat64(row, "size_bytes")

		labels := map[string]string{"schema": schema, "table": table, "index": index}
		pmc.collector.Gauge("postgresql_unused_index_size_bytes", sizeBytes, labels)
	}
}

// collectBackgroundWriterStats collects PostgreSQL background writer statistics
func (pmc *PostgreSQLMetricsCollector) collectBackgroundWriterStats(ctx context.Context) {
	query := `
		SELECT
			checkpoints_timed,
			checkpoints_req,
			checkpoint_write_time,
			checkpoint_sync_time,
			buffers_checkpoint,
			buffers_clean,
			maxwritten_clean,
			buffers_backend,
			buffers_backend_fsync,
			buffers_alloc
		FROM pg_stat_bgwriter
	`

	rows, err := pmc.queryRows(ctx, query)
	if err != nil || len(rows) == 0 {
		return
	}

	row := rows[0]
	stats := map[string]float64{
		"checkpoints_timed":     pmc.getFloat64(row, "checkpoints_timed"),
		"checkpoints_req":       pmc.getFloat64(row, "checkpoints_req"),
		"checkpoint_write_time": pmc.getFloat64(row, "checkpoint_write_time"),
		"checkpoint_sync_time":  pmc.getFloat64(row, "checkpoint_sync_time"),
		"buffers_checkpoint":    pmc.getFloat64(row, "buffers_checkpoint"),
		"buffers_clean":         pmc.getFloat64(row, "buffers_clean"),
		"maxwritten_clean":      pmc.getFloat64(row, "maxwritten_clean"),
		"buffers_backend":       pmc.getFloat64(row, "buffers_backend"),
		"buffers_backend_fsync": pmc.getFloat64(row, "buffers_backend_fsync"),
		"buffers_alloc":         pmc.getFloat64(row, "buffers_alloc"),
	}

	for statName, value := range stats {
		labels := map[string]string{"stat_type": statName}
		pmc.collector.Counter("postgresql_bgwriter_stats_total", labels)
		pmc.collector.Gauge("postgresql_bgwriter_"+statName, value, nil)
	}
}

// collectVacuumStats collects PostgreSQL vacuum statistics
func (pmc *PostgreSQLMetricsCollector) collectVacuumStats(ctx context.Context) {
	query := `
		SELECT
			schemaname,
			tablename,
			n_dead_tup,
			n_live_tup,
			CASE WHEN n_live_tup > 0 THEN n_dead_tup::float / n_live_tup ELSE 0 END as dead_tuple_ratio,
			last_vacuum,
			last_autovacuum,
			last_analyze,
			last_autoanalyze
		FROM pg_stat_user_tables
		WHERE n_dead_tup > 1000
		ORDER BY n_dead_tup DESC
		LIMIT 20
	`

	rows, err := pmc.queryRows(ctx, query)
	if err != nil {
		return
	}

	for _, row := range rows {
		schema := pmc.getString(row, "schemaname")
		table := pmc.getString(row, "tablename")
		deadTuples := pmc.getFloat64(row, "n_dead_tup")
		liveTuples := pmc.getFloat64(row, "n_live_tup")
		deadRatio := pmc.getFloat64(row, "dead_tuple_ratio")

		labels := map[string]string{"schema": schema, "table": table}
		pmc.collector.Gauge("postgresql_dead_tuples", deadTuples, labels)
		pmc.collector.Gauge("postgresql_live_tuples", liveTuples, labels)
		pmc.collector.Gauge("postgresql_dead_tuple_ratio", deadRatio, labels)
	}
}

// MySQLMetricsCollector collects MySQL-specific metrics
type MySQLMetricsCollector struct {
	collector MetricsCollector
	db        interface{}
}

// NewMySQLMetricsCollector creates a MySQL metrics collector
func NewMySQLMetricsCollector(collector MetricsCollector, db interface{}) *MySQLMetricsCollector {
	return &MySQLMetricsCollector{
		collector: collector,
		db:        db,
	}
}

// CollectMetrics collects MySQL-specific metrics
func (mmc *MySQLMetricsCollector) CollectMetrics(ctx context.Context) {
	mmc.collectInnoDBStats(ctx)
	mmc.collectReplicationStats(ctx)
	mmc.collectConnectionStats(ctx)
	mmc.collectTableStats(ctx)
	mmc.collectSlowQueries(ctx)
	mmc.collectBufferPoolStats(ctx)
}

// collectInnoDBStats collects MySQL InnoDB statistics
func (mmc *MySQLMetricsCollector) collectInnoDBStats(ctx context.Context) {
	query := "SHOW ENGINE INNODB STATUS"

	rows, err := mmc.queryRows(ctx, query)
	if err != nil || len(rows) == 0 {
		return
	}

	// Parse InnoDB status (simplified version)
	status := mmc.getString(rows[0], "Status")

	// Extract key metrics from status text (this is a simplified example)
	if strings.Contains(status, "Buffer pool hit rate") {
		// Parse buffer pool hit rate from status
		pmc.collector.Gauge("mysql_innodb_buffer_pool_hit_rate", 0.95, nil) // Placeholder
	}
}

// collectReplicationStats collects MySQL replication statistics
func (mmc *MySQLMetricsCollector) collectReplicationStats(ctx context.Context) {
	query := "SHOW SLAVE STATUS"

	rows, err := mmc.queryRows(ctx, query)
	if err != nil || len(rows) == 0 {
		return
	}

	row := rows[0]
	secondsBehindMaster := mmc.getFloat64(row, "Seconds_Behind_Master")

	if pc, ok := mmc.collector.(*PrometheusCollector); ok {
		pc.RecordMySQLReplicationDelay(secondsBehindMaster)
	} else {
		mmc.collector.Gauge("mysql_replication_delay_seconds", secondsBehindMaster, nil)
	}
}

// collectConnectionStats collects MySQL connection statistics
func (mmc *MySQLMetricsCollector) collectConnectionStats(ctx context.Context) {
	query := `
		SELECT
			VARIABLE_NAME,
			VARIABLE_VALUE
		FROM INFORMATION_SCHEMA.GLOBAL_STATUS
		WHERE VARIABLE_NAME IN ('Threads_connected', 'Threads_running', 'Max_used_connections')
	`

	rows, err := mmc.queryRows(ctx, query)
	if err != nil {
		return
	}

	for _, row := range rows {
		varName := mmc.getString(row, "VARIABLE_NAME")
		varValue := mmc.getFloat64(row, "VARIABLE_VALUE")

		metricName := strings.ToLower(strings.Replace(varName, "_", "_", -1))
		mmc.collector.Gauge("mysql_"+metricName, varValue, nil)
	}
}

// collectTableStats collects MySQL table statistics
func (mmc *MySQLMetricsCollector) collectTableStats(ctx context.Context) {
	query := `
		SELECT
			table_schema,
			table_name,
			data_length + index_length as size_bytes,
			table_rows
		FROM information_schema.tables
		WHERE table_schema NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
		ORDER BY size_bytes DESC
		LIMIT 20
	`

	rows, err := mmc.queryRows(ctx, query)
	if err != nil {
		return
	}

	for _, row := range rows {
		schema := mmc.getString(row, "table_schema")
		table := mmc.getString(row, "table_name")
		sizeBytes := mmc.getFloat64(row, "size_bytes")
		tableRows := mmc.getFloat64(row, "table_rows")

		labels := map[string]string{"database": schema, "table": table}
		mmc.collector.Gauge("mysql_table_size_bytes", sizeBytes, labels)
		mmc.collector.Gauge("mysql_table_rows", tableRows, labels)
	}
}

// collectSlowQueries collects MySQL slow query statistics
func (mmc *MySQLMetricsCollector) collectSlowQueries(ctx context.Context) {
	query := `
		SELECT VARIABLE_VALUE as slow_queries
		FROM INFORMATION_SCHEMA.GLOBAL_STATUS
		WHERE VARIABLE_NAME = 'Slow_queries'
	`

	rows, err := mmc.queryRows(ctx, query)
	if err != nil || len(rows) == 0 {
		return
	}

	slowQueries := mmc.getFloat64(rows[0], "slow_queries")

	if pc, ok := mmc.collector.(*PrometheusCollector); ok {
		// This would need to track incremental changes
		pc.RecordMySQLSlowQuery()
	} else {
		mmc.collector.Gauge("mysql_slow_queries_total", slowQueries, nil)
	}
}

// collectBufferPoolStats collects MySQL InnoDB buffer pool statistics
func (mmc *MySQLMetricsCollector) collectBufferPoolStats(ctx context.Context) {
	query := `
		SELECT
			VARIABLE_NAME,
			VARIABLE_VALUE
		FROM INFORMATION_SCHEMA.GLOBAL_STATUS
		WHERE VARIABLE_NAME LIKE 'Innodb_buffer_pool%'
	`

	rows, err := mmc.queryRows(ctx, query)
	if err != nil {
		return
	}

	for _, row := range rows {
		varName := mmc.getString(row, "VARIABLE_NAME")
		varValue := mmc.getFloat64(row, "VARIABLE_VALUE")

		statType := strings.TrimPrefix(strings.ToLower(varName), "innodb_buffer_pool_")
		labels := map[string]string{"stat_type": statType}
		mmc.collector.Gauge("mysql_buffer_pool_stats", varValue, labels)
	}
}

// SQLiteMetricsCollector collects SQLite-specific metrics
type SQLiteMetricsCollector struct {
	collector MetricsCollector
	db        interface{}
	dbPath    string
}

// NewSQLiteMetricsCollector creates a SQLite metrics collector
func NewSQLiteMetricsCollector(collector MetricsCollector, db interface{}) *SQLiteMetricsCollector {
	return &SQLiteMetricsCollector{
		collector: collector,
		db:        db,
	}
}

// CollectMetrics collects SQLite-specific metrics
func (smc *SQLiteMetricsCollector) CollectMetrics(ctx context.Context) {
	smc.collectDatabaseStats(ctx)
	smc.collectCacheStats(ctx)
	smc.collectWALStats(ctx)
	smc.collectPragmaSettings(ctx)
}

// collectDatabaseStats collects SQLite database statistics
func (smc *SQLiteMetricsCollector) collectDatabaseStats(ctx context.Context) {
	// File size
	query := "PRAGMA page_count; PRAGMA page_size;"

	if sqlxDB, ok := smc.db.(*sqlx.DB); ok {
		var pageCount, pageSize int64

		err := sqlxDB.GetContext(ctx, &pageCount, "PRAGMA page_count")
		if err == nil {
			err = sqlxDB.GetContext(ctx, &pageSize, "PRAGMA page_size")
			if err == nil {
				fileSize := pageCount * pageSize

				if pc, ok := smc.collector.(*PrometheusCollector); ok {
					pc.RecordSQLiteFileSize(fileSize)
				} else {
					smc.collector.Gauge("sqlite_file_size_bytes", float64(fileSize), nil)
					smc.collector.Gauge("sqlite_page_count", float64(pageCount), nil)
					smc.collector.Gauge("sqlite_page_size", float64(pageSize), nil)
				}
			}
		}
	}
}

// collectCacheStats collects SQLite cache statistics
func (smc *SQLiteMetricsCollector) collectCacheStats(ctx context.Context) {
	cacheQueries := map[string]string{
		"cache_size":       "PRAGMA cache_size",
		"cache_spill":      "PRAGMA cache_spill",
		"temp_store":       "PRAGMA temp_store",
		"mmap_size":        "PRAGMA mmap_size",
	}

	for statName, query := range cacheQueries {
		if sqlxDB, ok := smc.db.(*sqlx.DB); ok {
			var value int64
			err := sqlxDB.GetContext(ctx, &value, query)
			if err == nil {
				labels := map[string]string{"stat_type": statName}
				smc.collector.Gauge("sqlite_cache_stats", float64(value), labels)
			}
		}
	}
}

// collectWALStats collects SQLite WAL statistics
func (smc *SQLiteMetricsCollector) collectWALStats(ctx context.Context) {
	walQueries := map[string]string{
		"wal_autocheckpoint": "PRAGMA wal_autocheckpoint",
		"wal_checkpoint":     "PRAGMA wal_checkpoint(PASSIVE)",
	}

	for statName, query := range walQueries {
		if sqlxDB, ok := smc.db.(*sqlx.DB); ok {
			var value int64
			err := sqlxDB.GetContext(ctx, &value, query)
			if err == nil {
				labels := map[string]string{"stat_type": statName}
				smc.collector.Gauge("sqlite_wal_stats", float64(value), labels)
			}
		}
	}
}

// collectPragmaSettings collects SQLite PRAGMA settings
func (smc *SQLiteMetricsCollector) collectPragmaSettings(ctx context.Context) {
	pragmas := []string{
		"synchronous",
		"journal_mode",
		"locking_mode",
		"auto_vacuum",
		"foreign_keys",
	}

	for _, pragma := range pragmas {
		query := fmt.Sprintf("PRAGMA %s", pragma)
		if sqlxDB, ok := smc.db.(*sqlx.DB); ok {
			var value string
			err := sqlxDB.GetContext(ctx, &value, query)
			if err == nil {
				// Convert string values to numeric where possible
				numValue := 0.0
				switch value {
				case "ON", "on", "1", "true":
					numValue = 1.0
				case "OFF", "off", "0", "false":
					numValue = 0.0
				default:
					// Try to parse as number
					if val, err := strconv.ParseFloat(value, 64); err == nil {
						numValue = val
					}
				}

				labels := map[string]string{"pragma_name": pragma}
				smc.collector.Gauge("sqlite_pragma_settings", numValue, labels)
			}
		}
	}
}

// SQLServerMetricsCollector collects SQL Server-specific metrics
type SQLServerMetricsCollector struct {
	collector MetricsCollector
	db        interface{}
}

// NewSQLServerMetricsCollector creates a SQL Server metrics collector
func NewSQLServerMetricsCollector(collector MetricsCollector, db interface{}) *SQLServerMetricsCollector {
	return &SQLServerMetricsCollector{
		collector: collector,
		db:        db,
	}
}

// CollectMetrics collects SQL Server-specific metrics
func (ssmc *SQLServerMetricsCollector) CollectMetrics(ctx context.Context) {
	ssmc.collectBufferCacheStats(ctx)
	ssmc.collectWaitStats(ctx)
	ssmc.collectLockStats(ctx)
	ssmc.collectDatabaseSizes(ctx)
	ssmc.collectIndexFragmentation(ctx)
}

// collectBufferCacheStats collects SQL Server buffer cache statistics
func (ssmc *SQLServerMetricsCollector) collectBufferCacheStats(ctx context.Context) {
	query := `
		SELECT
			(cntr_value * 1.0 / base_cntr_value) * 100.0 as buffer_cache_hit_ratio
		FROM sys.dm_os_performance_counters
		WHERE counter_name = 'Buffer cache hit ratio'
		AND instance_name = ''
	`

	rows, err := ssmc.queryRows(ctx, query)
	if err != nil || len(rows) == 0 {
		return
	}

	hitRatio := ssmc.getFloat64(rows[0], "buffer_cache_hit_ratio")

	if pc, ok := ssmc.collector.(*PrometheusCollector); ok {
		pc.RecordSQLServerBufferCacheHitRatio(hitRatio)
	} else {
		ssmc.collector.Gauge("sqlserver_buffer_cache_hit_ratio", hitRatio, nil)
	}
}

// collectWaitStats collects SQL Server wait statistics
func (ssmc *SQLServerMetricsCollector) collectWaitStats(ctx context.Context) {
	query := `
		SELECT TOP 10
			wait_type,
			wait_time_ms,
			waiting_tasks_count
		FROM sys.dm_os_wait_stats
		WHERE wait_time_ms > 0
		ORDER BY wait_time_ms DESC
	`

	rows, err := ssmc.queryRows(ctx, query)
	if err != nil {
		return
	}

	for _, row := range rows {
		waitType := ssmc.getString(row, "wait_type")
		waitTime := ssmc.getFloat64(row, "wait_time_ms")
		waitingTasks := ssmc.getFloat64(row, "waiting_tasks_count")

		labels := map[string]string{"wait_type": waitType}
		ssmc.collector.Counter("sqlserver_wait_stats_total", labels)
		ssmc.collector.Gauge("sqlserver_wait_time_ms", waitTime, labels)
		ssmc.collector.Gauge("sqlserver_waiting_tasks", waitingTasks, labels)
	}
}

// collectLockStats collects SQL Server lock statistics
func (ssmc *SQLServerMetricsCollector) collectLockStats(ctx context.Context) {
	query := `
		SELECT
			resource_type,
			request_mode,
			COUNT(*) as lock_count
		FROM sys.dm_tran_locks
		GROUP BY resource_type, request_mode
	`

	rows, err := ssmc.queryRows(ctx, query)
	if err != nil {
		return
	}

	for _, row := range rows {
		resourceType := ssmc.getString(row, "resource_type")
		requestMode := ssmc.getString(row, "request_mode")
		lockCount := ssmc.getFloat64(row, "lock_count")

		labels := map[string]string{"lock_type": resourceType, "mode": requestMode}
		ssmc.collector.Gauge("sqlserver_lock_stats", lockCount, labels)
	}
}

// collectDatabaseSizes collects SQL Server database sizes
func (ssmc *SQLServerMetricsCollector) collectDatabaseSizes(ctx context.Context) {
	query := `
		SELECT
			DB_NAME(database_id) as database_name,
			type_desc,
			size * 8 * 1024 as size_bytes
		FROM sys.master_files
		WHERE database_id > 4
	`

	rows, err := ssmc.queryRows(ctx, query)
	if err != nil {
		return
	}

	for _, row := range rows {
		database := ssmc.getString(row, "database_name")
		fileType := ssmc.getString(row, "type_desc")
		sizeBytes := ssmc.getFloat64(row, "size_bytes")

		labels := map[string]string{"database": database, "file_type": fileType}
		ssmc.collector.Gauge("sqlserver_database_size_bytes", sizeBytes, labels)
	}
}

// collectIndexFragmentation collects SQL Server index fragmentation
func (ssmc *SQLServerMetricsCollector) collectIndexFragmentation(ctx context.Context) {
	query := `
		SELECT TOP 20
			DB_NAME() as database_name,
			OBJECT_NAME(ps.object_id) as table_name,
			i.name as index_name,
			ps.avg_fragmentation_in_percent
		FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'LIMITED') ps
		INNER JOIN sys.indexes i ON ps.object_id = i.object_id AND ps.index_id = i.index_id
		WHERE ps.avg_fragmentation_in_percent > 30
		ORDER BY ps.avg_fragmentation_in_percent DESC
	`

	rows, err := ssmc.queryRows(ctx, query)
	if err != nil {
		return
	}

	for _, row := range rows {
		database := ssmc.getString(row, "database_name")
		table := ssmc.getString(row, "table_name")
		index := ssmc.getString(row, "index_name")
		fragmentation := ssmc.getFloat64(row, "avg_fragmentation_in_percent")

		labels := map[string]string{"database": database, "table": table, "index": index}
		ssmc.collector.Gauge("sqlserver_index_fragmentation_percent", fragmentation, labels)
	}
}

// Helper methods for database queries

func (pmc *PostgreSQLMetricsCollector) queryRows(ctx context.Context, query string) ([]map[string]interface{}, error) {
	if pool, ok := pmc.db.(*pgxpool.Pool); ok {
		rows, err := pool.Query(ctx, query)
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		var result []map[string]interface{}
		for rows.Next() {
			values, err := rows.Values()
			if err != nil {
				continue
			}

			row := make(map[string]interface{})
			for i, col := range rows.FieldDescriptions() {
				row[col.Name] = values[i]
			}
			result = append(result, row)
		}
		return result, nil
	}

	if sqlxDB, ok := pmc.db.(*sqlx.DB); ok {
		rows, err := sqlxDB.QueryxContext(ctx, query)
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		var result []map[string]interface{}
		for rows.Next() {
			row := make(map[string]interface{})
			err := rows.MapScan(row)
			if err != nil {
				continue
			}
			result = append(result, row)
		}
		return result, nil
	}

	return nil, fmt.Errorf("unsupported database type")
}

func (mmc *MySQLMetricsCollector) queryRows(ctx context.Context, query string) ([]map[string]interface{}, error) {
	if sqlxDB, ok := mmc.db.(*sqlx.DB); ok {
		rows, err := sqlxDB.QueryxContext(ctx, query)
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		var result []map[string]interface{}
		for rows.Next() {
			row := make(map[string]interface{})
			err := rows.MapScan(row)
			if err != nil {
				continue
			}
			result = append(result, row)
		}
		return result, nil
	}
	return nil, fmt.Errorf("unsupported database type")
}

func (smc *SQLiteMetricsCollector) queryRows(ctx context.Context, query string) ([]map[string]interface{}, error) {
	// SQLite uses the same pattern as MySQL for sqlx
	return (&MySQLMetricsCollector{db: smc.db}).queryRows(ctx, query)
}

func (ssmc *SQLServerMetricsCollector) queryRows(ctx context.Context, query string) ([]map[string]interface{}, error) {
	// SQL Server uses the same pattern as MySQL for sqlx
	return (&MySQLMetricsCollector{db: ssmc.db}).queryRows(ctx, query)
}

// Helper methods for extracting values from query results

func (pmc *PostgreSQLMetricsCollector) getString(row map[string]interface{}, key string) string {
	if val, ok := row[key]; ok && val != nil {
		return fmt.Sprintf("%v", val)
	}
	return ""
}

func (pmc *PostgreSQLMetricsCollector) getFloat64(row map[string]interface{}, key string) float64 {
	if val, ok := row[key]; ok && val != nil {
		switch v := val.(type) {
		case float64:
			return v
		case int64:
			return float64(v)
		case int:
			return float64(v)
		case string:
			if f, err := strconv.ParseFloat(v, 64); err == nil {
				return f
			}
		}
	}
	return 0.0
}

func (mmc *MySQLMetricsCollector) getString(row map[string]interface{}, key string) string {
	return (&PostgreSQLMetricsCollector{}).getString(row, key)
}

func (mmc *MySQLMetricsCollector) getFloat64(row map[string]interface{}, key string) float64 {
	return (&PostgreSQLMetricsCollector{}).getFloat64(row, key)
}

func (smc *SQLiteMetricsCollector) getString(row map[string]interface{}, key string) string {
	return (&PostgreSQLMetricsCollector{}).getString(row, key)
}

func (smc *SQLiteMetricsCollector) getFloat64(row map[string]interface{}, key string) float64 {
	return (&PostgreSQLMetricsCollector{}).getFloat64(row, key)
}

func (ssmc *SQLServerMetricsCollector) getString(row map[string]interface{}, key string) string {
	return (&PostgreSQLMetricsCollector{}).getString(row, key)
}

func (ssmc *SQLServerMetricsCollector) getFloat64(row map[string]interface{}, key string) float64 {
	return (&PostgreSQLMetricsCollector{}).getFloat64(row, key)
}

// Additional helper methods for PrometheusCollector

func (pc *PrometheusCollector) RecordPostgreSQLConnections(state, database string, count int) {
	if pc.postgresMetrics != nil {
		pc.postgresMetrics.connectionsByState.WithLabelValues(state, database).Set(float64(count))
	}
}