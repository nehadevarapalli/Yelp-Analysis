/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.mycompany.topbusinesscategoriesbylocation;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author nehadevarapalli
 */
public class CategoryReducer extends Reducer<Text, CategoryStatsWritable, Text, Text> {
    public void reduce(Text key, Iterable<CategoryStatsWritable> values, Context context) throws IOException, InterruptedException {
        int totalReviews = 0;
        double totalStars = 0.0;
        int businessCount = 0;
        
        for (CategoryStatsWritable stat : values) {
            totalReviews += stat.getTotalReviews().get();
            totalStars += stat.getAvgStars().get();
            businessCount += stat.getBusinessCount().get();
        }
        
        double avgStars = totalStars / businessCount;
        
        String outputValue = String.format("Total Reviews: %d, Avg Rating: %.2f, Number of Businesses: %d", totalReviews, avgStars, businessCount);
        
        context.write(key, new Text(outputValue));
    }
}
