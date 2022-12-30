package dev.rabbit.rule;

import com.google.api.services.bigquery.model.TableRow;
import org.jetbrains.annotations.NotNull;
import org.reflections.Reflections;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public interface Rule extends Serializable, Comparable<Rule> {

    static List<Rule> getRules() throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        Iterator<Class<? extends Rule>> ruleSet = new Reflections("dev.rabbit").getSubTypesOf(Rule.class).iterator();
        List<Rule> rules = new ArrayList<>();
        while (ruleSet.hasNext()) rules.add(ruleSet.next().getConstructor().newInstance());
        Collections.sort(rules);
        return rules;
    }

    Iterable<TableRow> applyRule(Iterable<TableRow> t);

    FilterType getFilterType();

    default int compareTo(@NotNull Rule o) {
        return -1;
    }

    enum FilterType {
        ALL, BATCH, SINGLE
    }
}
