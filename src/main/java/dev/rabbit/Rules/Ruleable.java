package dev.rabbit.Rules;

import org.apache.beam.sdk.values.KV;

@FunctionalInterface
public interface Ruleable<T> {
    Iterable<KV<String, T>> applyRule(Iterable<KV<String, T>> t);
}
