package data.paloalto.categories;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class UniqueCategoriesReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
	public void reduce(Text key, Iterable<NullWritable> values, Context context)
			throws IOException, InterruptedException {
		
		NullWritable nullOb = NullWritable.get();
		context.write(key, nullOb);

	}
}
