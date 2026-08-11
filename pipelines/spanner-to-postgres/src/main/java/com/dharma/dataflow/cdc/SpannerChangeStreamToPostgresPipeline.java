package com.dharma.dataflow.cdc;

import com.google.cloud.Timestamp;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry point.
 *
 * This mirrors what the "Spanner_Change_Streams_to_BigQuery" Google-provided
 * template does under the hood, minus the parts specific to BigQuery. Instead
 * of just appending to a changelog table, this pipeline maintains *current*
 * state in Postgres via upsert/delete, which is the behavior we'll eventually
 * need for the real Oracle target too.
 *
 * Run with the DirectRunner (default if you don't pass --runner), which
 * executes entirely on this machine. That's exactly what we want here, since
 * the managed Dataflow *service* runs in Google's cloud and has no way to
 * reach a Postgres instance sitting behind your laptop's home network / NAT.
 */
public class SpannerChangeStreamToPostgresPipeline {

    private static final Logger LOG = LoggerFactory.getLogger(SpannerChangeStreamToPostgresPipeline.class);

    public static void main(String[] args) {
        PostgresSinkOptions options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .as(PostgresSinkOptions.class);

        Pipeline pipeline = Pipeline.create(options);

        SpannerConfig spannerConfig = SpannerConfig.create()
                .withProjectId(options.getSpannerProjectId())
                .withInstanceId(options.getSpannerInstanceId())
                .withDatabaseId(options.getSpannerDatabaseId());

        // Start watching from "now" rather than replaying the change stream's full
        // history (Spanner retains change stream data for a configurable window,
        // 1 day by default). For a POC we only care about changes made after we
        // hit run.
        Timestamp startTime = Timestamp.now();
        LOG.info("Starting change stream read from {}", startTime);

        PCollection<OrderChange> changes = pipeline
                .apply("ReadFromSpannerChangeStream", SpannerIO.readChangeStream()
                        .withSpannerConfig(spannerConfig)
                        .withChangeStreamName(options.getChangeStreamName())
                        .withMetadataInstance(options.getMetadataInstanceId())
                        .withMetadataDatabase(options.getMetadataDatabaseId())
                        .withInclusiveStartAt(startTime))
                .apply("ParseDataChangeRecords", ParDo.of(new DataChangeRecordToOrderChangeFn()));

        changes.apply("WriteToPostgres", ParDo.of(new PostgresWriterFn(
                options.getPostgresJdbcUrl(),
                options.getPostgresUsername(),
                options.getPostgresPassword())));

        pipeline.run().waitUntilFinish();
    }
}
