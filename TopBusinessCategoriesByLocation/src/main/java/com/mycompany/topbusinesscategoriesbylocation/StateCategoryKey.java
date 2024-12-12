/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.mycompany.topbusinesscategoriesbylocation;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class StateCategoryKey implements WritableComparable<StateCategoryKey> {
    private Text state;
    private Double compositeScore;
    
    public StateCategoryKey() {
        this.state = new Text();
        this.compositeScore = 0.0;
    }
    
    public StateCategoryKey (String state, double compositeScore) {
        this.state = new Text(state);
        this.compositeScore = compositeScore;
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        state.write(out);
        out.writeDouble(compositeScore);
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        state.readFields(in);
        compositeScore = in.readDouble();
    }
    
    @Override
    public int compareTo(StateCategoryKey o) {
        int cmp = this.state.compareTo(o.state);
        if (cmp == 0) {
            return -Double.compare(this.compositeScore, o.compositeScore); //Descending by score
        }
        return cmp;
    }
    
    public Text getState() { return state; }
    public double getCompositeScore() { return compositeScore; }
}