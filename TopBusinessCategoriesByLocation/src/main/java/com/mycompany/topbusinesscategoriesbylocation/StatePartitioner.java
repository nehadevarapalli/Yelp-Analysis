/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.mycompany.topbusinesscategoriesbylocation;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 *
 * @author nehadevarapalli
 */
public class StatePartitioner extends Partitioner<StateCategoryKey, Text> {
    @Override
    public int getPartition(StateCategoryKey key, Text value, int numPartitions) {
       return (key.getState().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }  
}
