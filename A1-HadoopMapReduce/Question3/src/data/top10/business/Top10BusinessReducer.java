package data.top10.business;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Top10BusinessReducer extends Reducer<Text, Text, Text, Text> {

	private TreeMap<String, Float> rating = new TreeMap<>();
	private ValueComparator valueComparator = new ValueComparator(rating);
	private TreeMap<String, Float> sortedMap = new TreeMap<String, Float>(valueComparator);

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		float sum = 0;
		float count = 0;
		for (Text value : values) {
			sum += Float.parseFloat(value.toString());
			count++;
		}
		float avg = new Float(sum / count);
		rating.put(key.toString(), avg);
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		sortedMap.putAll(rating);

		int count = 10;
		for (Entry<String, Float> entry : sortedMap.entrySet()) {
			if (count == 0) {
				break;
			}
			context.write(new Text(entry.getKey()), new Text(String.valueOf(entry.getValue())));
			count--;
		}
	}

}
