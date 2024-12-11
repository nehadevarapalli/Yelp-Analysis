/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.mycompany.sentimentanalysis;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONObject;

/**
 *
 * @author nehadevarapalli
 */
public class SentimentAnalysisMapper extends Mapper<LongWritable, Text, Text, Text> {
    private final Text businessId = new Text();
    private final Text sentimentScore = new Text();
    private final HashMap<String, Integer> afinnMap = new HashMap<>();
    private URI[] files;
    
    @Override
    protected void setup(Context context) throws IOException {
        files = DistributedCache.getCacheFiles(context.getConfiguration());
        System.out.println("files:"+ files);
        Path path = new Path(files[0]);
        FileSystem fs = FileSystem.get(context.getConfiguration());
        FSDataInputStream in = fs.open(path);
        BufferedReader br = new BufferedReader(new InputStreamReader(in));
        String line = "";
        while ((line = br.readLine()) != null) {
            String parts[] = line.split("\t");
            if (parts.length == 2) {
            afinnMap.put(parts[0].toLowerCase(), Integer.parseInt(parts[1]));
            }
        }
        br.close();
        in.close();
    }
    
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String businessIdStr;
        String reviewText;
        String line = value.toString();
        try{
            JSONObject obj = new JSONObject(line);
            businessIdStr = obj.getString("business_id");
            reviewText = obj.getString("text");
            
            businessId.set(businessIdStr);
            int score = 0;
            
            for (String word : reviewText.toLowerCase().replaceAll("[^a-zA-Z ]", "").split("\\s+")) {
                score += afinnMap.getOrDefault(word, 0);
            }
            
            sentimentScore.set("SCORE: " + score);
            context.write(businessId, sentimentScore);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
