package data.top10.business;

import java.util.Comparator;
import java.util.Map;

class ValueComparator implements Comparator<Object> {

	Map<String, Float> map;

	public ValueComparator(Map<String, Float> map) {
		this.map = map;
	}

	public int compare(Object keyA, Object keyB) {

		Float a = (Float) map.get(keyA);
		Float b = (Float) map.get(keyB);

		int compare = b.compareTo(a);

		if (compare == 0)
			return 1;
		return compare;
	}
}