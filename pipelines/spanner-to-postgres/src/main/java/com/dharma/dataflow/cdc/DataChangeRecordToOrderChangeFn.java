package com.dharma.dataflow.cdc;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.DataChangeRecord;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.Mod;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.ModType;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Every DataChangeRecord that comes out of SpannerIO.readChangeStream() can bundle
 * up multiple row-level "Mods" in a single Spanner transaction. This DoFn unpacks
 * each Mod into a flat, easy-to-use OrderChange.
 *
 * We only care about the Orders1 table here, everything else (including Dataflow's
 * own internal metadata tables) gets filtered out.
 */
public class DataChangeRecordToOrderChangeFn extends DoFn<DataChangeRecord, OrderChange> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(DataChangeRecordToOrderChangeFn.class);
    private static final String WATCHED_TABLE = "Orders1";

    // Not Serializable, so we create one per bundle instead of storing it on the DoFn instance
    private transient ObjectMapper objectMapper;

    @Setup
    public void setup() {
        objectMapper = new ObjectMapper();
    }

    @ProcessElement
    public void processElement(@Element DataChangeRecord record, OutputReceiver<OrderChange> out) {
        if (!WATCHED_TABLE.equals(record.getTableName())) {
            return;
        }

        String commitTimestamp = record.getCommitTimestamp().toString();
        ModType modType = record.getModType();

        for (Mod mod : record.getMods()) {
            try {
                JsonNode keys = objectMapper.readTree(mod.getKeysJson());
                String orderId = textOrNull(keys, "OrderId");

                if (orderId == null) {
                    LOG.warn("Skipping mod with no OrderId in keysJson: {}", mod.getKeysJson());
                    continue;
                }

                if (modType == ModType.DELETE) {
                    // A deleted row is already gone from Spanner by the time we read this,
                    // so only the primary key survives. Everything else is genuinely unknown.
                    out.output(new OrderChange(orderId, null, null, null, null,
                            modType.name(), commitTimestamp));
                    continue;
                }

                JsonNode newValues = objectMapper.readTree(mod.getNewValuesJson());
                String customerName = textOrNull(newValues, "CustomerName");
                String orderDate = textOrNull(newValues, "OrderDate");
                String amount = textOrNull(newValues, "Amount");
                String status = textOrNull(newValues, "Status");

                out.output(new OrderChange(orderId, customerName, orderDate, amount,
                        status, modType.name(), commitTimestamp));

            } catch (Exception e) {
                // A POC-friendly choice: log and skip the bad record rather than crashing
                // the whole pipeline. In production you'd route this to a dead-letter queue.
                LOG.error("Failed to parse mod, skipping. keysJson={} newValuesJson={}",
                        mod.getKeysJson(), mod.getNewValuesJson(), e);
            }
        }
    }

    private static String textOrNull(JsonNode node, String field) {
        JsonNode value = node.get(field);
        return (value == null || value.isNull()) ? null : value.asText();
    }
}
