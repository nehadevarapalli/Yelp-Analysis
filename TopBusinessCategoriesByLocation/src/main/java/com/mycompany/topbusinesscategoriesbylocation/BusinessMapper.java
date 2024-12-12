/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.mycompany.topbusinesscategoriesbylocation;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 *
 * @author nehadevarapalli
 */

// Mapper to process business.json
public class BusinessMapper extends Mapper<LongWritable, Text, Text, CategoryStatsWritable> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            JSONObject business = new JSONObject(value.toString());
            
            // Extracting location information
            String state = business.optString("state", "Unknown").trim();
            String locationKey = state;
            
            int reviewCount = business.optInt("review_count", 0);
            double stars = business.optDouble("stars", 0.0);
            String categoriesStr = business.optString("categories", null);
            if (categoriesStr != null && !categoriesStr.isEmpty()) {
                String[] categories = categoriesStr.split(", ");
                for (String category : categories) {
                    String outputKey = locationKey.trim() + "|" + category.trim();
                    CategoryStatsWritable stats = new CategoryStatsWritable(reviewCount, stars, 1);
                    context.write(new Text(outputKey), stats);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
