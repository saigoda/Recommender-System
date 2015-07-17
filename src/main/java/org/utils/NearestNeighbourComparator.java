package org.utils;

import java.util.Comparator;

public class NearestNeighbourComparator implements Comparator<String> {
	
		@Override
		public int compare(String o1, String o2) {
			int s1 = Integer.parseInt(o1.split(":")[2]);
			int s2 = Integer.parseInt(o2.split(":")[2]);
			if (s1 < s2)
				return 1;
			else if (s1 > s2)
				return -1;
			else
				return 0;
		}

	
	

}
