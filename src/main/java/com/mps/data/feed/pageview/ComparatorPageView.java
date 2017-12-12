package com.mps.data.feed.pageview;

import java.util.Comparator;

public class ComparatorPageView implements Comparator<DataPageView>{

	@Override
	public int compare(DataPageView u1, DataPageView u2){
        return u1.getReg_exp().compareTo(u2.getReg_exp());
    }
}
