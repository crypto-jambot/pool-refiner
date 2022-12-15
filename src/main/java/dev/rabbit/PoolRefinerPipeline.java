package dev.rabbit;

import com.google.api.client.json.GenericJson;
import com.google.api.services.bigquery.model.TableRow;
import dev.rabbit.Rules.InactivePool;
import dev.rabbit.Rules.Ruleable;
import dev.rabbit.transform.FilterPools;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.List;

public class PoolRefinerPipeline {
    public static void run(PoolRefinerPipelineOptions options) {

        Pipeline pipeline = Pipeline.create(options);

        List<Ruleable<TableRow>> rules = List.of(new InactivePool());

        pipeline.apply("Query for initial set of pools", BigQueryIO.readTableRows()
                        .fromQuery(options.getQueryString())
                        .withoutValidation()
                        .usingStandardSql())
                .apply("Apply a set of rules that filter out undesirable pools", FilterPools.of(rules))
                .apply(MapElements.into(TypeDescriptors.strings()).via(GenericJson::toString))
                .apply(TextIO.write().to("output/test.jsonl").withoutSharding());

        pipeline.run();
    }
}
