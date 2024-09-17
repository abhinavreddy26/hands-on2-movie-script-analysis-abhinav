package com.movie.script.analysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DialogueLengthReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
    int accumulatedLength = 0;

    // Summing up all the values associated with the key
    for (IntWritable value : values) {
        accumulatedLength += value.get();
    }

    // Writing the key and the total length to the context
    context.write(key, new IntWritable(accumulatedLength));
}

