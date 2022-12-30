package dev.rabbit.transform;

import com.google.api.services.bigquery.model.TableRow;
import dev.rabbit.doFn.AllDoFn;
import dev.rabbit.doFn.BatchDoFn;
import dev.rabbit.doFn.SingleDoFn;
import dev.rabbit.rule.Rule;
import lombok.AllArgsConstructor;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.PCollection;

import java.util.List;

@AllArgsConstructor
public class FilterPools extends PTransform<PCollection<TableRow>, PCollection<TableRow>> {

    private final List<Rule> rules;
    private final ValueProvider<Integer> maxRpcBatchSize;

    public static FilterPools of(List<Rule> rules, ValueProvider<Integer> maxRPCBatchSize) {
        return new FilterPools(rules, maxRPCBatchSize);
    }

    @Override
    public PCollection<TableRow> expand(PCollection<TableRow> input) {
        PCollection<TableRow> collection = input;
        for (Rule rule : rules) {
            switch (rule.getFilterType()) {
                case BATCH:
                    collection = collection
                            .apply(WithKeys.of(""))
                            .apply(ParDo.of(BatchDoFn.of(maxRpcBatchSize, rule)))
                            .apply(Values.create());
                    break;
                case ALL:
                    collection = collection
                            .apply(Combine.globally(CombineToList.of()))
                            .apply(ParDo.of(AllDoFn.of(rule)));
                    break;
                case SINGLE:
                    collection = collection
                            .apply(ParDo.of(SingleDoFn.of(rule)));
            }
        }
        return collection;
    }
}
