package com.svlugovoy.words;

import org.apache.beam.sdk.transforms.SimpleFunction;

import java.util.Arrays;
import java.util.List;

public class ExtractWords extends SimpleFunction<String, List<String>> {

    @Override
    public List<String> apply(String input) {
        return Arrays.asList(input.split(" "));
    }
}
