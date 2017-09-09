package data.top10.business;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Top10BusinessReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
	HashMap<String, Float> rating = new HashMap<String, Float>();

	@Override
	protected void reduce(Text key, Iterable<FloatWritable> value, Context context)
			throws IOException, InterruptedException {

		FloatWritable average = new FloatWritable(0);
		int total = 0;
		float count = 0;

		for (FloatWritable star : value) {
			total += star.get();
			count++;
		}

		float avg = total / count;
		average.set(avg);
		rating.put(key.toString(), avg);
	}

	@Override
	protected void cleanup(Reducer<Text, FloatWritable, Text, FloatWritable>.Context context)
			throws IOException, InterruptedException {

		Map<String, Float> sortedMap = new TreeMap<String, Float>(new ValueComparator(rating));
		sortedMap.putAll(rating);

		int i = 0;

		for (Map.Entry<String, Float> entry : sortedMap.entrySet()) {
			context.write(new Text(entry.getKey()), new FloatWritable(entry.getValue()));
			i++;
			if (i == 10)
				break;
		}
	}

}
