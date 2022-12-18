package dev.rabbit.Rules;

import com.google.api.services.bigquery.model.TableRow;

import java.io.Serializable;

public class InactivePool implements Rule, Serializable {
    @Override
    public Iterable<TableRow> applyRule(Iterable<TableRow> rows) {
        for (TableRow row : rows) {
            row.set("test1", "test1");
        }
        return rows;
    }
}
