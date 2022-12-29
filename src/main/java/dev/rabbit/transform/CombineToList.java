package dev.rabbit.transform;

import org.apache.beam.sdk.transforms.Combine;

import java.util.ArrayList;
import java.util.List;

public class CombineToList<T> extends Combine.CombineFn<T, List<T>, List<T>> {

    public static <T> CombineToList<T> of() {
        return new CombineToList<>();
    }

    @Override
    public List<T> createAccumulator() {
        return new ArrayList<>();
    }

    @Override
    public List<T> addInput(List<T> accumulator, T input) {
        accumulator.add(input);
        return accumulator;
    }

    @Override
    public List<T> mergeAccumulators(Iterable<List<T>> accumulators) {
        List<T> merged = new ArrayList<>();
        for (List<T> list : accumulators) {
            merged.addAll(list);
        }
        return merged;
    }

    @Override
    public List<T> extractOutput(List<T> accumulator) {
        return accumulator;
    }
}
