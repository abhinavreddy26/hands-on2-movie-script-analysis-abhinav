package com.movie.script.analysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CharacterWordReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

   @Override
public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
    int totalCount = 0;
    
    // Summing up all the values associated with the key
    for (IntWritable value : values) {
        totalCount += value.get();
    }
    
    // Writing the key and its corresponding sum to the context
    context.write(key, new IntWritable(totalCount));
}

