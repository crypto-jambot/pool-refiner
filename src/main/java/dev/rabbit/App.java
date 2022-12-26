package dev.rabbit;

import dev.rabbit.rule.Rule;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

import java.util.List;

public class App {
    public static void main(String[] args) throws Exception {
        List<Rule> rules = Rule.getRules();
        PoolRefinerPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(PoolRefinerPipelineOptions.class);
        PoolRefinerPipeline.run(options, rules);
    }
}
