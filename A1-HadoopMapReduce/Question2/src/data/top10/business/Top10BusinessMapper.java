package data.top10.business;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Top10BusinessMapper extends Mapper<LongWritable, Text, Text, FloatWritable>{

	static String record = "";

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		Text business_id = new Text();
		FloatWritable stars = new FloatWritable(1);

		record = record.concat(value.toString());
		String[] fields = record.split("::");
		
		if (fields.length == 4) {
			business_id.set(fields[2].trim());
			stars.set(Float.parseFloat(fields[3].trim()));
			context.write(business_id, stars);
			record = "";
		}
	}
}
