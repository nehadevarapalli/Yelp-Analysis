/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.mycompany.staffactivitydriver;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONObject;

/**
 *
 * @author nehadevarapalli
 */
public class StaffActivityMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final Text businessDayHourKey = new Text();
    private final IntWritable one = new IntWritable(1);
    private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        try {
            // Parse the input JSON
            JSONObject obj = new JSONObject(value.toString());
            String businessId = obj.getString("business_id");
            String date = obj.getString("date");

            // Split the comma-separated list of timestamps
            String[] timestamps = date.split(", ");

            // Process each timestamp in the list
            for (String timestamp : timestamps) {
                // Parse the timestamp into a Calendar object
                Calendar calendar = Calendar.getInstance();
                calendar.setTime(dateFormat.parse(timestamp));

                // Get the day of the week (1 = Sunday, 7 = Saturday)
                int dayOfWeek = calendar.get(Calendar.DAY_OF_WEEK);

                // Create the key using businessId, dayOfWeek
                businessDayHourKey.set(businessId + ":" + dayOfWeek);

                // Emit the key with value '1' (indicating one check-in for this hour)
                context.write(businessDayHourKey, one);
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
}