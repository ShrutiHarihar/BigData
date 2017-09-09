package data.paloalto.categories;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UniqueCategoriesMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
	static String record = "";

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		Text business_category = new Text();
		record = record.concat(value.toString());
		String[] fields = record.split("::");
		if (fields.length == 3) {
			if ((fields[1].contains("Palo Alto"))) {
				String[] category_line = fields[2].toString().split("(List\\()|(\\))|(,)");
				for (String category : category_line) {
					business_category.set(category.trim());
					NullWritable nullObject = NullWritable.get();
					context.write(business_category, nullObject);
				}
			}
			record = "";
		}
	}

}
