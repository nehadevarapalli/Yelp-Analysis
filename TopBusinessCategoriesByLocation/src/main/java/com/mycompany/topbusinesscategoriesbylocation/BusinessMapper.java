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
public class BusinessMapper extends Mapper<LongWritable, Text, StateCategoryKey, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            JSONObject business = new JSONObject(value.toString());
            
            // Extracting location information
            String state = business.optString("state", "Unknown").trim();
            String categoriesStr = business.optString("categories", null);
            
            int reviewCount = business.optInt("review_count", 0);
            double stars = business.optDouble("stars", 0.0);
            double compositeScore = (0.7 * reviewCount) + (0.3 * stars);
            
            if (categoriesStr != null && !categoriesStr.isEmpty()) {
                String[] categories = categoriesStr.split(",");
                for (String category : categories) {
                    context.write(new StateCategoryKey(state, compositeScore), new Text(category));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
