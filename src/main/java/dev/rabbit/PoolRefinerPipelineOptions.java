package dev.rabbit;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;

import static org.apache.beam.sdk.options.Validation.Required;

public interface PoolRefinerPipelineOptions extends PipelineOptions {
    @Required
    @Default.String("crypto-351521")
    ValueProvider<String> getGcpProject();

    void setGcpProject(ValueProvider<String> value);

    @Required
    @Default.String("SELECT * FROM `crypto-351521.ethereum_v2.unrefined_pools`")
    ValueProvider<String> getQueryString();

    void setQueryString(ValueProvider<String> value);

    @Default.Integer(100)
    ValueProvider<Integer> getRpcBatchSize();

    void setRpcBatchSize(ValueProvider<Integer> value);

    @Required
    @Default.String("crypto-351521:ethereum_v2.refined_pools")
    ValueProvider<String> getDestinationTable();

    void setDestinationTable(ValueProvider<String> value);
}
