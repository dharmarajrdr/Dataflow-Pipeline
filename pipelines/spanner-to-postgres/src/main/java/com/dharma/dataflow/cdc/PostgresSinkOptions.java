package com.dharma.dataflow.cdc;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;

/**
 * Every flag this pipeline accepts on the command line.
 *
 * Beam turns each getter/setter pair into a "--flagName=value" CLI argument
 * automatically, no extra parsing code needed. GcpOptions is what gives us
 * "--project=..." for authenticating against Spanner using your Application
 * Default Credentials (the ones set up by `gcloud auth application-default login`).
 */
public interface PostgresSinkOptions extends PipelineOptions, GcpOptions {

    @Description("GCP project that owns the Spanner instance, e.g. dharma-learn-gcp")
    @Validation.Required
    String getSpannerProjectId();
    void setSpannerProjectId(String value);

    @Description("Source Spanner instance ID, e.g. spanner-instance")
    @Validation.Required
    String getSpannerInstanceId();
    void setSpannerInstanceId(String value);

    @Description("Source Spanner database ID, e.g. spanner-database")
    @Validation.Required
    String getSpannerDatabaseId();
    void setSpannerDatabaseId(String value);

    @Description("Name of the Change Stream to read from, e.g. cs_orders1")
    @Validation.Required
    String getChangeStreamName();
    void setChangeStreamName(String value);

    @Description("Spanner instance to store Dataflow's own checkpoint/metadata in. "
            + "Usually the same instance as the source.")
    @Validation.Required
    String getMetadataInstanceId();
    void setMetadataInstanceId(String value);

    @Description("Spanner database to store Dataflow's own checkpoint/metadata in. "
            + "Usually the same database as the source.")
    @Validation.Required
    String getMetadataDatabaseId();
    void setMetadataDatabaseId(String value);

    @Description("Postgres JDBC URL, e.g. jdbc:postgresql://localhost:5432/orders_cdc_poc")
    @Validation.Required
    String getPostgresJdbcUrl();
    void setPostgresJdbcUrl(String value);

    @Description("Postgres username")
    @Default.String("dharmaraj")
    String getPostgresUsername();
    void setPostgresUsername(String value);

    @Description("Postgres password (blank if your local Postgres has no password set)")
    @Default.String("")
    String getPostgresPassword();
    void setPostgresPassword(String value);
}
