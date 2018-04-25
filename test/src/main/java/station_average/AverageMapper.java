package station_average;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class AverageMapper extends Mapper<Object, Text, Text, AverageWritable> {

	@Override
	protected void map(Object key, Text value,
			Mapper<Object, Text, Text, AverageWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String line = value.toString();
		if(line.indexOf("ENDTIME") >= 0)
			return;
		String arr[] = line.split(",");
		if(arr.length == 7) {
			FloatWritable station_num = new FloatWritable(Math.abs(Integer.parseInt(arr[5].replace("\"",""))-Integer.parseInt(arr[4].replace("\"",""))));
			System.out.println(station_num);
			AverageWritable temp_result = new AverageWritable();
			temp_result.setCount(new IntWritable(1));
			temp_result.setTemp(station_num);
			context.write(new Text(arr[1]), temp_result);
		}
	}
}
