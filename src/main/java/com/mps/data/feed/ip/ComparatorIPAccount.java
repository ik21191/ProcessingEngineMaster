/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mps.data.feed.ip;

import java.util.Comparator;

/**
 *
 * @author kapil.verma
 */
public class ComparatorIPAccount implements Comparator<DataIPAccount> {
    public int compare(DataIPAccount ipSet1, DataIPAccount ipSet2) {
        return ipSet1.ipBegin.compareTo(ipSet2.ipBegin);
    }
};
