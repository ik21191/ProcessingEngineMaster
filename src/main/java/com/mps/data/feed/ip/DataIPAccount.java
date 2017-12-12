/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mps.data.feed.ip;

import java.math.BigInteger;

/**
 *
 * @author kapil.verma
 */
public class DataIPAccount{
    public int ipType = -2;
    public BigInteger ipBegin;
    public BigInteger ipEnd;
    public String startIP = "";
    public String endIP = "";
    public String range = "";
    public String data = "";
    public String remarks = "";

    @Override
    public String toString() {
        return "IpData{" + "ipType=" + ipType + ", ipBegin=" + ipBegin + ", ipEnd=" + ipEnd + ", StartIP=" + startIP + ", endIP=" + endIP + ", range=" + range + ", data=" + data + ", remarks=" + remarks + '}';
    }
    
}
