/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.mycompany.topbusinesscategoriesbylocation;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author nehadevarapalli
 */
public class CategoryReducer extends Reducer<StateCategoryKey, Text, Text, Text> {
    @Override
    public void reduce (StateCategoryKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        HashMap<String, Integer> stateCount = new HashMap<>();
        Text currentState = key.getState();
        int categoryCount = 0;
    
        for (Text category : values) {
            if (categoryCount < 3) {
                String formattedScore = String.format("%.2f", key.getCompositeScore());
                context.write(new Text(currentState), new Text(category.toString() + " | Composite Score: " + formattedScore));
                categoryCount++;
            } else {
                break;
            }
        }
    }
}
