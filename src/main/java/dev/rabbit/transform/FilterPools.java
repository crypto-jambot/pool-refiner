package dev.rabbit.transform;

import com.google.api.services.bigquery.model.TableRow;
import dev.rabbit.Rules.Rule;
import lombok.AllArgsConstructor;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import java.util.List;

@AllArgsConstructor
public class FilterPools extends PTransform<PCollection<TableRow>, PCollection<TableRow>> {

    private final List<Rule<TableRow>> rules;
    private final ValueProvider<Integer> maxRPCBatchSize;

    public static FilterPools of(List<Rule<TableRow>> rules, ValueProvider<Integer> maxRPCBatchSize) {
        return new FilterPools(rules, maxRPCBatchSize);
    }

    @Override
    public PCollection<TableRow> expand(PCollection<TableRow> input) {
        PCollection<KV<String, TableRow>> collection = input.apply(WithKeys.of(""));
        for (Rule<TableRow> rule : rules) {
            collection = collection.apply(ParDo.of(
                    new DoFn<KV<String, TableRow>, KV<String, TableRow>>() {
                        @StateId("buffer")
                        private final StateSpec<BagState<TableRow>> buffer = StateSpecs.bag();
                        @StateId("count")
                        private final StateSpec<ValueState<Integer>> count = StateSpecs.value();

                        @ProcessElement
                        public void processElement(@Element KV<String, TableRow> element,
                                                   OutputReceiver<KV<String, TableRow>> out,
                                                   @StateId("buffer") BagState<TableRow> bufferState,
                                                   @StateId("count") ValueState<Integer> countState) {
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
            ));
        }
        return collection.apply(Values.create());
    }
}
