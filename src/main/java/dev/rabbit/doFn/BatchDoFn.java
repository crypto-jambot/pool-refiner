package dev.rabbit.doFn;

import com.google.api.services.bigquery.model.TableRow;
import dev.rabbit.rule.Rule;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

import java.io.IOException;

public class BatchDoFn extends DoFn<KV<String, TableRow>, KV<String, TableRow>> {
    @StateId("buffer")
    private final StateSpec<BagState<TableRow>> buffer = StateSpecs.bag();
    @StateId("count")
    private final StateSpec<ValueState<Integer>> count = StateSpecs.value();
    private final Rule rule;
    private final ValueProvider<Integer> maxRPCBatchSize;

    private BatchDoFn(ValueProvider<Integer> maxRPCBatchSize, Rule rule) {
        this.maxRPCBatchSize = maxRPCBatchSize;
        this.rule = rule;
    }

    public static BatchDoFn of(ValueProvider<Integer> maxRPCBatchSize, Rule rule) {
        return new BatchDoFn(maxRPCBatchSize, rule);
    }

    @ProcessElement
    public void processElement(@Element KV<String, TableRow> element,
                               OutputReceiver<KV<String, TableRow>> out,
                               @StateId("buffer") BagState<TableRow> bufferState,
                               @StateId("count") ValueState<Integer> countState) throws IOException {
        int count = countState.read() != null ? countState.read() : 0;
        count++;
        countState.write(count);
        bufferState.add(element.getValue());

        if (count >= maxRPCBatchSize.get()) {
            Iterable<TableRow> filtered = rule.applyRule(bufferState.read());
            for (TableRow row : filtered) out.output(KV.of("", row));
            bufferState.clear();
            countState.clear();
        }
    }
}
