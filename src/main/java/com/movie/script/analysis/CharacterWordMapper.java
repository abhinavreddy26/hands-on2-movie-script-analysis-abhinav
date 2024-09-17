package com.movie.script.analysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class CharacterWordMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private Text characterWord = new Text();

    @Override
public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    String lineContent = value.toString();
    String[] lineParts = lineContent.split(":");

    // Ensure the line has exactly two parts (character and dialogue)
    if (lineParts.length == 2) {
        String characterName = lineParts[0].trim();
        String dialogueText = lineParts[1].trim().toLowerCase();

        StringTokenizer dialogueTokenizer = new StringTokenizer(dialogueText);
        
        // Iterate through the tokens in the dialogue
        while (dialogueTokenizer.hasMoreTokens()) {
            String wordToken = dialogueTokenizer.nextToken().replaceAll("[^a-zA-Z]", "");
            
            // Proceed only if the token is not empty
            if (!wordToken.isEmpty()) {
                word.set(wordToken);
                characterWord.set(characterName + ":" + word);
                context.write(characterWord, one);
            }
        }
    }
}

