/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.mycompany.sentimentanalysis;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONObject;

/**
 *
 * @author nehadevarapalli
 */
public class BusinessInfoMapper extends Mapper<Object, Text, Text, Text> {
    private final Text businessId = new Text();
    private final Text businessInfo = new Text();
    
    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        try {
            JSONObject obj = new JSONObject(line);
            String businessIdStr = obj.getString("business_id");
            String businessName = obj.getString("name");
            String businessCity = obj.getString("city");
            String businessState = obj.getString("state");
            double stars = obj.getDouble("stars");
            int reviewCount = obj.getInt("review_count");
            
            businessId.set(businessIdStr);
            businessInfo.set("INFO:" + businessName + " | " + businessCity + ", " + businessState + " | Stars: " + stars);
            context.write(businessId, businessInfo);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
