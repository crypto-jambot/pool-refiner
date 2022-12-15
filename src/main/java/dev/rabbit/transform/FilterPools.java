package dev.rabbit.transform;

import com.google.api.services.bigquery.model.TableRow;
import dev.rabbit.Rules.Ruleable;
import lombok.AllArgsConstructor;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import java.util.List;

@AllArgsConstructor
public class FilterPools extends PTransform<PCollection<TableRow>, PCollection<TableRow>> {

    List<Ruleable<TableRow>> rules;

    public static FilterPools of(List<Ruleable<TableRow>> rules) {
        return new FilterPools(rules);
    }

    @Override
    public PCollection<TableRow> expand(PCollection<TableRow> input) {
        PCollection<KV<String, TableRow>> collection = input.apply(WithKeys.of(""));
        for (Ruleable<TableRow> rule : rules) {
//
//            collection = collection.apply(ParDo.of(
//                    new DoFn<KV<String, TableRow>, KV<String, TableRow>>() {
//                        private static final int MAX_BUFFER_SIZE = 500;
//                        @StateId("buffer")
//                        private final StateSpec<BagState<KV<String, TableRow>>> bufferTableRows = StateSpecs.bag();
//                        @StateId("count")
//                        private final StateSpec<ValueState<Integer>> countState = StateSpecs.value();
//
//                        @ProcessElement
//                        public void processElement(ProcessContext c,
//                                                   @StateId("buffer") BagState<KV<String, TableRow>> bufferState,
//                                                   @StateId("count") ValueState<Integer> countState) {
//                            int count = countState.read() != null ? countState.read() : 0;
//                            count++;
//                            countState.write(count);
//                            bufferState.add(c.element());
//
//                            if (count >= MAX_BUFFER_SIZE) {
//                                Iterable<KV<String, TableRow>> filtered = rule.applyRule(bufferState.read());
//                                StreamSupport.stream(filtered.spliterator(), true).forEach(c::output);
////                                for (KV<String, TableRow> tableRow : rule.applyRule(bufferState.read())) {
////                                    c.output(tableRow);
////                                }
//                                bufferState.clear();
//                                countState.clear();
//                            }
//                        }
//                    }
//            ));
        }
        return collection.apply(Values.create());
    }
}
