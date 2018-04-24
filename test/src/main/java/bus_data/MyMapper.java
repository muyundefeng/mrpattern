package bus_data;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<Object, Text, IntWritable, EarliestAndLastetLongWritable> {

	@Override
	protected void map(Object key, Text value,
			Mapper<Object, Text, IntWritable, EarliestAndLastetLongWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String line = value.toString();
		if (line.indexOf("BEGINTIME") >= 0)
			return;
		String arr[] = line.split(",");
		if (arr.length == 7) {
			LongWritable start_time = new LongWritable(Long.parseLong(arr[2].replace("\"", "")));
			LongWritable end_time = new LongWritable(Long.parseLong(arr[3].replace("\"", "")));
			IntWritable station_nums = new IntWritable(
					Math.abs(Integer.parseInt(arr[4].replace("\"", "")) - Integer.parseInt(arr[5].replace("\"", ""))));
			EarliestAndLastetLongWritable myWritable = new EarliestAndLastetLongWritable();
			myWritable.setStart_time(start_time);
			myWritable.setEnd_time(end_time);
			myWritable.setLongest(station_nums);
//			System.out.println(myWritable);
			context.write(new IntWritable(1), myWritable);
		}
	}

}
