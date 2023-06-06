package dev.rabbit.rule;

import com.google.api.services.bigquery.model.TableRow;
import org.jetbrains.annotations.NotNull;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static dev.rabbit.util.ListUtils.toList;

public class DeadEndPools implements Rule {

    private static HashMap<String, Integer> deriveTokens(List<TableRow> pools) {
        HashMap<String, Integer> tokens = new HashMap<>();
        pools.forEach(pool -> {
            String token0Address = (String) pool.get("token0Address");
            String token1Address = (String) pool.get("token1Address");
            addTokens(tokens, token0Address, token1Address);
        });
        return tokens;
    }

    private static HashMap<String, Integer> filterTokens(Map<String, Integer> tokens, Predicate<Map.Entry<String, Integer>> predicate) {
        Stream<Map.Entry<String, Integer>> stream = tokens.entrySet().stream().filter(predicate);
        return (HashMap<String, Integer>) stream.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public static void addTokens(HashMap<String, Integer> tokens, String... addressParams) {
        List<String> addresses = List.of(addressParams);
        addresses.forEach(address -> {
            if (tokens.containsKey(address)) {
                tokens.put(address, tokens.get(address) + 1);
            } else {
                tokens.put(address, 1);
            }
        });
    }

    @Override
    public FilterType getFilterType() {
        return FilterType.ALL;
    }

    @Override
    public Iterable<TableRow> applyRule(Iterable<TableRow> elements) {
        List<TableRow> rows = toList(elements);
        HashMap<String, Integer> deadTokens = filterTokens(deriveTokens(rows), token -> token.getValue() == 1);

        while (deadTokens.size() > 0) {
            HashMap<String, Integer> deadTokensCopy = deadTokens;
            rows.removeIf(row -> deadTokensCopy.containsKey((String) row.get("token0Address")) || deadTokensCopy.containsKey((String) row.get("token1Address")));
            deadTokens = filterTokens(deriveTokens(rows), token -> token.getValue() == 1);
        }
        return rows;
    }

    @Override
    public int compareTo(@NotNull Rule o) {
        return 1;
    }
}
