package middle_num;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MiddleMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable>{

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, IntWritable, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String line = value.toString();
		if(line.indexOf("ENDTIME") >= 0)
			return;
		String arr[] = line.split(",");
		if(arr.length == 7) {
			IntWritable station_num = new IntWritable(Math.abs(Integer.parseInt(arr[5].replace("\"",""))-Integer.parseInt(arr[4].replace("\"",""))));
			context.write(new IntWritable(1), station_num);
		}
	}
	

}
