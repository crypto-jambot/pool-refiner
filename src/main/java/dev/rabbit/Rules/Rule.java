package dev.rabbit.Rules;

import com.google.api.services.bigquery.model.TableRow;
import org.reflections.Reflections;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

@FunctionalInterface
public interface Rule {
    static List<Rule> getRules() throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        Iterator<Class<? extends Rule>> ruleSet = new Reflections("dev.rabbit").getSubTypesOf(Rule.class).iterator();
        List<Rule> rules = new ArrayList<>();
        while (ruleSet.hasNext()) rules.add(ruleSet.next().getConstructor().newInstance());
        return rules;
    }

    Iterable<TableRow> applyRule(Iterable<TableRow> t);
}
