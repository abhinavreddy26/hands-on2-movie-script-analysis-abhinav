package com.movie.script.analysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class DialogueLengthMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable wordCount = new IntWritable();
    private Text character = new Text();

     @Override
public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    String lineContent = value.toString();
    String[] lineParts = lineContent.split(":");

    // Ensure the line is split into exactly two parts
    if (lineParts.length == 2) {
        character.set(lineParts[0].trim());
        String dialogueText = lineParts[1].trim();

        StringTokenizer dialogueTokenizer = new StringTokenizer(dialogueText);
        wordCount.set(dialogueTokenizer.countTokens());

        // Write the character and word count to the context
        context.write(character, wordCount);
    }
}
