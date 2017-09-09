package data.top10.business;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Top10Business {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		Configuration configuration = new Configuration();
		String[] otherArguments = new GenericOptionsParser(configuration, args).getRemainingArgs();

		if (otherArguments.length != 4) {
			System.err.println("Invalid number of arguments");
			System.exit(0);
		}

		Job job1 = Job.getInstance(configuration, "Top10 Business");
		job1.setJarByClass(Top10Business.class);
		job1.setMapperClass(Top10BusinessMapper.class);
		job1.setReducerClass(Top10BusinessReducer.class);

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job1, new Path(otherArguments[0]));
		FileOutputFormat.setOutputPath(job1, new Path(otherArguments[2]));

		boolean isJob1Completed = job1.waitForCompletion(true);

		if (isJob1Completed) {
			Configuration configuration2 = new Configuration();
			Job job2 = Job.getInstance(configuration2, "Business details");
			job2.setJarByClass(Top10Business.class);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);
			job2.setInputFormatClass(TextInputFormat.class);
			job2.setOutputFormatClass(TextOutputFormat.class);

			MultipleInputs.addInputPath(job2, new Path(args[2]), TextInputFormat.class,
					Top10BusinessRatingMapper.class);
			MultipleInputs.addInputPath(job2, new Path(args[1]), TextInputFormat.class, Top10BusinessDetailsMapper.class);

			job2.setReducerClass(Top10BusinessDetailReducer.class);
			FileOutputFormat.setOutputPath(job2, new Path(args[3]));

			job2.waitForCompletion(true);
		}
	}
}
