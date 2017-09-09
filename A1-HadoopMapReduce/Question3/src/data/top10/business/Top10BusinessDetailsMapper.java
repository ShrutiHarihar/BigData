package data.top10.business;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Top10BusinessDetailsMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String businessData[] = value.toString().split("::");
		context.write(new Text(businessData[0].trim()), new Text(
				"T2|" + businessData[0].trim() + "|" + businessData[1].trim() + "|" + businessData[2].trim()));

	}
}
