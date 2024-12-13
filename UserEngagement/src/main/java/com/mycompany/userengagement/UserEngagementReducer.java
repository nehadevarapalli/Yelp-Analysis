/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.mycompany.userengagement;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author nehadevarapalli
 */
public class UserEngagementReducer extends Reducer<Text, Text, Text, Text> {
    private TreeMap<Double, String> topUsersMap = new TreeMap<>();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            double score = Double.parseDouble(key.toString());
            topUsersMap.put(score, value.toString());

            // Keep only top N users in the TreeMap
            if (topUsersMap.size() > 10) {
                topUsersMap.remove(topUsersMap.firstKey());
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Double score : topUsersMap.descendingKeySet()) {
            String userInfo = topUsersMap.get(score);
            context.write(new Text(String.format("%.2f", score)), new Text(userInfo));
        }
    }
}
