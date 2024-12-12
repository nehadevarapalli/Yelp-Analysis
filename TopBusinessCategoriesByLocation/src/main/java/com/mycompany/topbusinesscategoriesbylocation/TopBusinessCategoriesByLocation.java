/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 */

package com.mycompany.topbusinesscategoriesbylocation;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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
public class TopBusinessCategoriesByLocation extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TopBusinessCategoriesByLocation(), args);
        
    }

    @Override
    public int run(String[] args) throws Exception{
        Configuration conf = getConf();
        
        if (args.length != 2) {
            System.err.println("Usage: TopBusinessCategories <business-input-path> <output-path>");
            System.exit(-1);
        }
        
        Job job = Job.getInstance(conf, "Top Business Categories by Location");
        
        job.setJarByClass(TopBusinessCategoriesByLocation.class);
        job.setMapperClass(BusinessMapper.class);
        job.setReducerClass(CategoryReducer.class);
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(CategoryStatsWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        
        return 0;
    }
}
