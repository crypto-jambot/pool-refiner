package dev.rabbit.Rules;

@FunctionalInterface
public interface Rule<T> {
    Iterable<T> applyRule(Iterable<T> t);
}
