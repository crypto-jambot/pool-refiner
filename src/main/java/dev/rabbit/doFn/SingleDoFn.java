package dev.rabbit.doFn;

import com.google.api.services.bigquery.model.TableRow;
import dev.rabbit.rule.Rule;
import org.apache.beam.sdk.transforms.DoFn;

import java.util.Iterator;
import java.util.List;

public class SingleDoFn extends DoFn<TableRow, TableRow> {
    private final Rule rule;

    private SingleDoFn(Rule rule) {
        this.rule = rule;
    }

    public static SingleDoFn of(Rule rule) {
        return new SingleDoFn(rule);
    }

    @ProcessElement
    public void processElement(@Element TableRow element, OutputReceiver<TableRow> out) {
        Iterable<TableRow> row = rule.applyRule(List.of(element));
        if (row == null) return;
        Iterator<TableRow> rowIterator = row.iterator();
        if (rowIterator.hasNext()) out.output(rowIterator.next());
    }
}