package dev.rabbit;

import com.google.api.client.json.GenericJson;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

public class PoolRefinerPipeline {
    public static void run(PoolRefinerPipelineOptions options) {

        Pipeline pipeline = Pipeline.create(options);

        PCollection<String> rows = pipeline
                .apply(BigQueryIO.readTableRows()
                        .fromQuery(options.getQueryString())
                        .withoutValidation()
                        .usingStandardSql())
                .apply(MapElements.into(TypeDescriptors.strings()).via(GenericJson::toString));
        rows.apply(TextIO.write().to("output/test.jsonl").withoutSharding());

        pipeline.run();
    }
}
