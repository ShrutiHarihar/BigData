package data.top10.business;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Top10BusinessRatingMapper extends Mapper<LongWritable, Text, Text, Text> {
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String line = value.toString().trim();
		String[] detail = line.split("\t");
		String bId = detail[0].trim();
		String rating = detail[1].trim();
		context.write(new Text(bId), new Text("T1|" + bId + "|" + rating));

	}
}
