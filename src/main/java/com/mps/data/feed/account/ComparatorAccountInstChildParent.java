package com.mps.data.feed.account;

import java.util.Comparator;

public class ComparatorAccountInstChildParent  implements Comparator<DataAccountInstChildParent> {

	@Override
	public int compare(DataAccountInstChildParent o1, DataAccountInstChildParent o2) {
		 return o1.institutionId.compareTo(o2.institutionId);
	}

}
