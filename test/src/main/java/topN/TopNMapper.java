package topN;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/*
 * get top n pattern
 */

public class TopNMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

	public static TreeMap<Integer, String> map = new TreeMap<Integer, String>();
	@Override
	protected void cleanup(Mapper<LongWritable, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		for(String part:map.values()) {
			context.write(NullWritable.get(), new Text(part));
		}
	}

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String line = value.toString();
		if(line.indexOf("STATIONFROM")>0)
			return;
		int stp_num = Integer.parseInt(line.split(",")[4].replace("\"", ""));
		if(map.size()>10) {
			map.remove(map.firstKey());
		}else {
			map.put(stp_num, line);
		}
	}

}
