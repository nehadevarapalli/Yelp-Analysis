/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.mycompany.topbusinesscategoriesbylocation;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

/**
 *
 * @author nehadevarapalli
 */
public class CategoryStatsWritable implements Writable {
    private IntWritable totalReviews;
    private DoubleWritable avgStars;
    private IntWritable businessCount;
    
    public CategoryStatsWritable() {
        this.totalReviews = new IntWritable(0);
        this.avgStars = new DoubleWritable(0.0);
        this.businessCount = new IntWritable(0);
    }
    
    public CategoryStatsWritable(int totalReviews, double avgStars, int businessCount) {
        this.totalReviews = new IntWritable(totalReviews);
        this.avgStars = new DoubleWritable(avgStars);
        this.businessCount = new IntWritable(businessCount);
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        totalReviews.write(out);
        avgStars.write(out);
        businessCount.write(out);
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        totalReviews.readFields(in);
        avgStars.readFields(in);
        businessCount.readFields(in);
    }
    
    public IntWritable getTotalReviews() { return totalReviews; }
    public DoubleWritable getAvgStars() { return avgStars; }
    public IntWritable getBusinessCount() { return businessCount; }
}
