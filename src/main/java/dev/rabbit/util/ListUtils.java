package dev.rabbit.util;

import com.google.api.services.bigquery.model.TableRow;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ListUtils {

    public static List<TableRow> toList(Iterable<TableRow> rows) {
        if (rows instanceof List) return new ArrayList<>((List<TableRow>) rows);
        List<TableRow> rowsList = new ArrayList<>();
        rows.forEach(rowsList::add);
        return rowsList;
    }
}
