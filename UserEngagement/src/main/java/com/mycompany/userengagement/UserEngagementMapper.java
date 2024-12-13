/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.mycompany.userengagement;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONObject;

/**
 *
 * @author nehadevarapalli
 */
public class UserEngagementMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        try {
            JSONObject jsonObject = new JSONObject(value.toString());

            String userId = jsonObject.getString("user_id");
            String name = jsonObject.optString("name", "Unknown");
            int reviewCount = jsonObject.optInt("review_count", 0);
            int fans = jsonObject.optInt("fans", 0);

            // Calculate total compliments
            int totalCompliments = 0;
            for (String field : jsonObject.keySet()) {
                if (field.startsWith("compliment_")) {
                    totalCompliments += jsonObject.optInt(field, 0);
                }
            }

            // Compute engagement score
            double engagementScore = 0.5 * reviewCount + 0.3 * totalCompliments + 0.2 * fans;

            String userInfo = "User ID: " + userId + ", Name: " + name + ", Review Count: " + reviewCount + ", Total Compliments: " + totalCompliments + ", Fans: " + fans;
            context.write(new Text(String.valueOf(engagementScore)), new Text(userInfo));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
