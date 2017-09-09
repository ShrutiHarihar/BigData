package data.paloalto.categories;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class UniqueCategories {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration configuration = new Configuration();
		String[] otherArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();
		if (otherArgs.length == 2) {

			Job job = Job.getInstance(configuration, "unique categories");
			job.setJarByClass(UniqueCategories.class);

			Path inputFile = new Path(otherArgs[0]);
			Path outputFile = new Path(otherArgs[1]);

			FileInputFormat.addInputPath(job, inputFile);
			FileOutputFormat.setOutputPath(job, outputFile);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);

			job.setMapperClass(UniqueCategoriesMapper.class);
			job.setCombinerClass(UniqueCategoriesReducer.class);
			job.setReducerClass(UniqueCategoriesReducer.class);

			System.exit(job.waitForCompletion(true) ? 1 : 0);
		} else {
			
            System.out.println("Invalid number of arguments");
			System.exit(0);
		}
	}

}
