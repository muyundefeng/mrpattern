package station_average;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AverageReducer extends Reducer<Text, AverageWritable, Text, AverageWritable> {

	@Override
	protected void reduce(Text arg0, Iterable<AverageWritable> arg1,
			Reducer<Text, AverageWritable, Text, AverageWritable>.Context arg2)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		int count = 0;
		float total = 0;
		for(AverageWritable temp:arg1) {
			count+=temp.getCount().get();
			total+=temp.getTemp().get()*temp.getCount().get();
		}
		float average = total/count;
		AverageWritable myAverageWritable = new AverageWritable();
		myAverageWritable.setCount(new IntWritable(count));
		myAverageWritable.setTemp(new FloatWritable(average));
		arg2.write(arg0, myAverageWritable);
	}
	

}
