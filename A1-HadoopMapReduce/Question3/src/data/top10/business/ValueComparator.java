package data.top10.business;

import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;

class ValueComparator implements Comparator<String> {

	TreeMap<String, Float> mMap;

	public ValueComparator(TreeMap<String, Float> lMap) {
		this.mMap = lMap;
	}

	@Override
	public int compare(String a, String b) {
		if (mMap.get(a) >= mMap.get(b)) {
			return -1;
		} else {
			return 1;
		}
	}

}