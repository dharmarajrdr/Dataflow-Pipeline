package com.dharma.dataflow.cdc;

import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Date;

/**
 * This is the piece that makes Postgres (and later, Oracle) different from the
 * BigQuery leg of this pipeline. BigQuery's template just appends every change
 * as a new row, an event log. A real operational database is expected to hold
 * *current* state, so here we translate each change into either:
 *
 *   - INSERT ... ON CONFLICT (order_id) DO UPDATE   for INSERT/UPDATE events
 *   - DELETE FROM orders2 WHERE order_id = ?         for DELETE events
 *
 * One connection per worker thread, opened once in @Setup and reused for every
 * element, then closed in @Teardown. Fine for a POC; a production version would
 * use a pooled DataSource (e.g. HikariCP) instead of a raw DriverManager connection.
 */
public class PostgresWriterFn extends DoFn<OrderChange, Void> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(PostgresWriterFn.class);

    private static final String UPSERT_SQL =
            "INSERT INTO orders2 (order_id, customer_name, order_date, amount, status, last_spanner_commit_ts) "
                    + "VALUES (?, ?, ?, ?, ?, ?) "
                    + "ON CONFLICT (order_id) DO UPDATE SET "
                    + "customer_name = EXCLUDED.customer_name, "
                    + "order_date = EXCLUDED.order_date, "
                    + "amount = EXCLUDED.amount, "
                    + "status = EXCLUDED.status, "
                    + "last_spanner_commit_ts = EXCLUDED.last_spanner_commit_ts "
                    // Guards against processing an older event after a newer one has already
                    // landed, which can happen with at-least-once delivery/retries.
                    + "WHERE EXCLUDED.last_spanner_commit_ts >= orders2.last_spanner_commit_ts";

    private static final String DELETE_SQL =
            "DELETE FROM orders2 WHERE order_id = ?";

    private final String jdbcUrl;
    private final String username;
    private final String password;

    private transient Connection connection;
    private transient PreparedStatement upsertStatement;
    private transient PreparedStatement deleteStatement;

    public PostgresWriterFn(String jdbcUrl, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
    }

    @Setup
    public void setup() throws SQLException {
        connection = DriverManager.getConnection(jdbcUrl, username, password);
        connection.setAutoCommit(true);
        upsertStatement = connection.prepareStatement(UPSERT_SQL);
        deleteStatement = connection.prepareStatement(DELETE_SQL);
        LOG.info("Postgres connection opened: {}", jdbcUrl);
    }

    @ProcessElement
    public void processElement(@Element OrderChange change) {
        try {
            if ("DELETE".equals(change.getModType())) {
                deleteStatement.setString(1, change.getOrderId());
                int rows = deleteStatement.executeUpdate();
                LOG.info("DELETE OrderId={} -> {} row(s) removed", change.getOrderId(), rows);
                return;
            }

            upsertStatement.setString(1, change.getOrderId());
            upsertStatement.setString(2, change.getCustomerName());
            upsertStatement.setDate(3, toSqlDate(change.getOrderDate()));
            upsertStatement.setBigDecimal(4, toBigDecimal(change.getAmount()));
            upsertStatement.setString(5, change.getStatus());
            upsertStatement.setString(6, change.getSpannerCommitTimestamp());

            int rows = upsertStatement.executeUpdate();
            LOG.info("{} OrderId={} -> {} row(s) written", change.getModType(), change.getOrderId(), rows);

        } catch (SQLException e) {
            // POC-friendly: log and move on rather than killing the whole pipeline
            // over one bad record. In production, route to a dead-letter table/topic.
            LOG.error("Failed to write change to Postgres: {}", change, e);
        }
    }

    @Teardown
    public void teardown() throws SQLException {
        if (upsertStatement != null) upsertStatement.close();
        if (deleteStatement != null) deleteStatement.close();
        if (connection != null) connection.close();
        LOG.info("Postgres connection closed");
    }

    private static Date toSqlDate(String isoDate) {
        return isoDate == null ? null : Date.valueOf(isoDate);
    }

    private static BigDecimal toBigDecimal(String value) {
        return value == null ? null : new BigDecimal(value);
    }
}
