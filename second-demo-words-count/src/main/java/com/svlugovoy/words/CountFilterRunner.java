package com.svlugovoy.words;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;

public class CountFilterRunner {

    public static void main(String... args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        pipeline
                .apply(TextIO.read().from("./input.txt"))
                .apply(new CountWords())
//                .apply(ParDo.of(new DoFn<KV<String, Long>, KV<String, Long>>() {
//                    @ProcessElement
//                    public void processElement(@Element KV<String, Long> input, OutputReceiver<KV<String, Long>> outputReceiver) {
//                        if(input.getValue() > 4) {
//                            outputReceiver.output(input);
//                        }
//                    }
//                }))
                .apply(Filter.by(kv -> kv.getValue() > 4))
                .apply(MapElements.into(TypeDescriptor.of(String.class)).via(
                        kv -> String.format("%s: %d", kv.getKey(), kv.getValue())))
                .apply(TextIO.write().to("./result/output-filter-count.txt")
                        .withoutSharding());

        // run the pipeline and wait until the whole pipeline is finished
        pipeline.run().waitUntilFinish();
    }

}
