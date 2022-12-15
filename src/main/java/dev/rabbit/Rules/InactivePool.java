package dev.rabbit.Rules;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.values.KV;

import java.io.Serializable;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class InactivePool implements Ruleable<TableRow>, Serializable {
    @Override
    public Iterable<KV<String, TableRow>> applyRule(Iterable<KV<String, TableRow>> rows) {
        return StreamSupport.stream(rows.spliterator(), true).map(stringTableRowKV -> KV.of("", stringTableRowKV.getValue().set("testField1", "testValue1"))).collect(Collectors.toList());
    }
}
