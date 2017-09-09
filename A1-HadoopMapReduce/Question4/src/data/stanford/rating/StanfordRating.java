package data.stanford.rating;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class StanfordRating {

	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration configuration = new Configuration();

		String[] otherArguments = new GenericOptionsParser(configuration, args).getRemainingArgs();

		Job job = Job.getInstance(configuration, "stanford business");
		job.addCacheFile(new Path(otherArguments[1]).toUri());
		job.setJarByClass(StanfordRating.class);
		job.setMapperClass(StanfordRatingMapper.class);
		job.setNumReduceTasks(0);

		FileInputFormat.setInputPaths(job, new Path(otherArguments[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArguments[2]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
