/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.mycompany.sentimentanalysis;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author nehadevarapalli
 */
public class SentimentAnalysisDriver extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new SentimentAnalysisDriver(), args);
        System.exit(res);
    }
    
    @Override
    public int run(String[] args) throws Exception {
//        Configuration conf = new Configuration();
        Configuration conf = getConf();
        
        if (args.length != 4) {
            System.err.println("Usage: SentimentAnalysis <input-path> <output-path>");
            System.exit(-1);
        }
        
        // Adding AFINN file to Distributed Cache
        DistributedCache.addCacheFile(new URI("/AFINN-111.txt"), conf);
        Job job = Job.getInstance(conf, "Sentiment Analysis");
        
        job.setJarByClass(SentimentAnalysisDriver.class);
        job.setMapperClass(SentimentAnalysisMapper.class);
        job.setReducerClass(SentimentAnalysisReducer.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        
        return 0;
    }
}
