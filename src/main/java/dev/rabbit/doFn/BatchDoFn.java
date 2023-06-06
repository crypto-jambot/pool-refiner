package dev.rabbit.doFn;

import com.google.api.services.bigquery.model.TableRow;
import dev.rabbit.rule.Rule;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;

public class BatchDoFn extends DoFn<KV<String, TableRow>, TableRow> {
    @StateId("buffer")
    private final StateSpec<BagState<TableRow>> buffer = StateSpecs.bag();
    @StateId("count")
    private final StateSpec<ValueState<Integer>> count = StateSpecs.value();
    @TimerId("expiry")
    private final TimerSpec expirySpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);
    private final Rule rule;
    private final ValueProvider<Integer> maxRpcBatchSize;

    private BatchDoFn(ValueProvider<Integer> maxRpcBatchSize, Rule rule) {
        this.maxRpcBatchSize = maxRpcBatchSize;
        this.rule = rule;
    }

    public static BatchDoFn of(ValueProvider<Integer> maxRpcBatchSize, Rule rule) {
        return new BatchDoFn(maxRpcBatchSize, rule);
    }

    @ProcessElement
    public void processElement(@Element KV<String, TableRow> element,
                               OutputReceiver<TableRow> out,
                               BoundedWindow window,
                               @StateId("buffer") BagState<TableRow> bufferState,
                               @StateId("count") ValueState<Integer> countState,
                               @TimerId("expiry") Timer expiryTimer) {
        expiryTimer.set(window.maxTimestamp());
        int count = countState.read() != null ? countState.read() : 0;
        count++;
        countState.write(count);
        bufferState.add(element.getValue());

        if (count >= maxRpcBatchSize.get()) {
            Iterable<TableRow> filtered = rule.applyRule(bufferState.read());
            for (TableRow row : filtered) out.output(row);
            bufferState.clear();
            countState.clear();
        }
    }

    @OnTimer("expiry")
    public void onExpiry(
            OnTimerContext context,
            @StateId("buffer") BagState<TableRow> bufferState,
            @StateId("count") ValueState<Integer> countState) {
        if (!bufferState.isEmpty().read()) {
            Iterable<TableRow> filtered = rule.applyRule(bufferState.read());
            for (TableRow row : filtered) context.output(row);
            bufferState.clear();
            countState.clear();
        }
    }
}
