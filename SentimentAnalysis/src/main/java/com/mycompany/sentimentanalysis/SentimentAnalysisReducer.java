/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.mycompany.sentimentanalysis;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author nehadevarapalli
 */
public class SentimentAnalysisReducer extends Reducer<Text, Text, Text, Text> {
    private final Text result = new Text();
    
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String businessInfo = null;
        int totalSentimentScore = 0;
        
        for (Text val : values) {
            String value = val.toString();
            if (value.startsWith("INFO:")) {
                businessInfo = value.substring(5);
            } else if (value.startsWith("SCORE:")) {
                totalSentimentScore += Integer.parseInt(value.substring(6).trim());
            }
        }
        
        if (businessInfo != null) {
            result.set(businessInfo + " | Sentiment Score: " + totalSentimentScore);
            context.write(key, result);
        }
    }
}
