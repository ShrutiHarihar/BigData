package data.top10.business;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Top10Business {

	public static void main(String args[]) throws ClassNotFoundException, IOException, InterruptedException {
		Configuration configuration = new Configuration();
		String[] otherArguments = new GenericOptionsParser(configuration, args).getRemainingArgs();

		Job job = Job.getInstance(configuration, "Top 10 Business");

		job.setJarByClass(Top10Business.class);

		Path inputFile = new Path(otherArguments[0]);
		Path outputFile = new Path(otherArguments[1]);

		FileInputFormat.addInputPath(job, inputFile);
		FileOutputFormat.setOutputPath(job, outputFile);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FloatWritable.class);

		job.setMapperClass(Top10BusinessMapper.class);
		job.setReducerClass(Top10BusinessReducer.class);

		FileInputFormat.setMinInputSplitSize(job, 500000000);

		System.exit(job.waitForCompletion(true) ? 1 : 0);

	}

}
