package dev.rabbit.doFn;

import com.google.api.services.bigquery.model.TableRow;
import dev.rabbit.rule.Rule;
import org.apache.beam.sdk.transforms.DoFn;

import java.util.List;

public class AllDoFn extends DoFn<List<TableRow>, TableRow> {

    private final Rule rule;

    private AllDoFn(Rule rule) {
        this.rule = rule;
    }

    public static AllDoFn of(Rule rule) {
        return new AllDoFn(rule);
    }

    @ProcessElement
    public void processElement(@Element List<TableRow> element, OutputReceiver<TableRow> out) {
        Iterable<TableRow> filtered = rule.applyRule(element);
        filtered.forEach(out::output);
    }
}
