package dev.rabbit;

import dev.rabbit.Rules.Rule;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.List;

public class App {
    public static void main(String[] args) throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        List<Rule> rules = Rule.getRules();
        PoolRefinerPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(PoolRefinerPipelineOptions.class);
        PoolRefinerPipeline.run(options, rules);
    }
}
