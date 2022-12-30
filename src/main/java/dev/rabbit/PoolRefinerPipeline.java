package dev.rabbit;

import dev.rabbit.rule.Rule;
import dev.rabbit.transform.FilterPools;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;

import java.util.List;

public class PoolRefinerPipeline {
    public static void run(PoolRefinerPipelineOptions options, List<Rule> rules) {

        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply("Query for initial set of pools", BigQueryIO.readTableRows()
                        .fromQuery(options.getQueryString())
                        .withoutValidation()
                        .usingStandardSql())
                .apply("Apply a set of rules that filter out undesirable pools", FilterPools.of(rules, options.getRpcBatchSize()))
                .apply("Write remaining pools to the refined pools table after truncating.", BigQueryIO.writeTableRows()
                        .to(options.getDestinationTable())
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));

        pipeline.run();
    }
}
