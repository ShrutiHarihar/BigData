package data.top10.business;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Top10BusinessDetailReducer extends Reducer<Text, Text, Text, Text> {

	private ArrayList<String> top10Business = new ArrayList<String>();
	private ArrayList<String> top10businessDetails = new ArrayList<String>();

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		for (Text text : values) {
			String value = text.toString();
			if (value.startsWith("T1")) {
				top10Business.add(value.substring(3));
			} else {
				top10businessDetails.add(value.substring(3));
			}
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		for (String topBusiness : top10Business) {
			for (String detail : top10businessDetails) {
				String[] split1 = topBusiness.split("\\|");
				String split1BusinessId = split1[0].trim();

				String[] split2 = detail.split("\\|");
				String split2businessId = split2[0].trim();

				if (split1BusinessId.equals(split2businessId)) {
					context.write(new Text(split1BusinessId), new Text(split2[1] + "\t" + split2[2] + "\t" + split1[1]));
					break;
				}
			}
		}
	}

}
