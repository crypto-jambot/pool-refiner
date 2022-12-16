package dev.rabbit;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;

import static org.apache.beam.sdk.options.Validation.Required;

public interface PoolRefinerPipelineOptions extends PipelineOptions {
    @Required
    @Default.String("SELECT * FROM `crypto-351521.ethereum_v2.pre_refinement_pools`")
    ValueProvider<String> getQueryString();

    void setQueryString(ValueProvider<String> value);

    @Default.Integer(500)
    ValueProvider<Integer> getRPCBatchSize();

    void setRPCBatchSize(ValueProvider<Integer> value);
}
