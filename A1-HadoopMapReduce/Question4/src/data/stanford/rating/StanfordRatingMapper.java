package data.stanford.rating;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StanfordRatingMapper extends Mapper<LongWritable, Text, Text, Text> {

	private ArrayList<String> businessIds = new ArrayList<String>();

	@Override
	protected void setup(Context context) throws IOException {

		Path[] files = context.getLocalCacheFiles();
		for (Path file : files) {
			FileReader fr = new FileReader(file.toString());
			BufferedReader br = new BufferedReader(fr);
			String line = null;
			while ((line = br.readLine()) != null) {
				String[] fields = line.split("::");
				if (fields[1].toLowerCase().contains("stanford")) {
					businessIds.add(fields[0]);
				}
			}
		}
	}

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String line = value.toString();
		String[] fields = line.split("::");
		String user_Id = fields[1];
		String business_Id = fields[2];
		String rating = fields[3];
		if (businessIds.contains(business_Id))
			context.write(new Text(user_Id), new Text(rating));
	}
}
