package dev.rabbit;

import org.apache.beam.sdk.options.PipelineOptionsFactory;

public class App {
    public static void main(String[] args) {
        PoolRefinerPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(PoolRefinerPipelineOptions.class);
        PoolRefinerPipeline.run(options);
    }
}
