package com.svlugovoy.words;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptor;

public class CountRunner {

    public static void main(String... args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        pipeline
                .apply(TextIO.read().from("./input.txt"))
                .apply(new CountWords())
//                .apply(MapElements.via(new SimpleFunction<KV<String, Long>, String>() {
//                    @Override
//                    public String apply(KV<String, Long> input) {
//                        return String.format("%s: %d", input.getKey(), input.getValue());
//                    }
//                }))
                .apply(MapElements.into(TypeDescriptor.of(String.class)).via(
                        kv -> String.format("%s: %d", kv.getKey(), kv.getValue())))
                .apply(TextIO.write().to("./result/output-count.txt").withoutSharding());

        // run the pipeline and wait until the whole pipeline is finished
        pipeline.run().waitUntilFinish();
    }

}
