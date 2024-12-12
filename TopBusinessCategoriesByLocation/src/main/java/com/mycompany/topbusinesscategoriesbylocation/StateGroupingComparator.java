/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.mycompany.topbusinesscategoriesbylocation;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 *
 * @author nehadevarapalli
 */
public class StateGroupingComparator extends WritableComparator {
    protected StateGroupingComparator() {
        super(StateCategoryKey.class, true);
    }
    
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        StateCategoryKey k1 = (StateCategoryKey) w1;
        StateCategoryKey k2 = (StateCategoryKey) w2;
        return k1.getState().compareTo(k2.getState());
    }
}
