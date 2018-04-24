package bus_data;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer
		extends Reducer<IntWritable, EarliestAndLastetLongWritable, IntWritable, EarliestAndLastetLongWritable> {

	@Override
	protected void reduce(IntWritable arg0, Iterable<EarliestAndLastetLongWritable> arg1,
			Reducer<IntWritable, EarliestAndLastetLongWritable, IntWritable, EarliestAndLastetLongWritable>.Context arg2)
			throws IOException, InterruptedException {
		System.out.println("==========================");
		// TODO Auto-generated method stub
		LongWritable earlist = new LongWritable(Long.MAX_VALUE);
		LongWritable lated = new LongWritable(Long.MIN_VALUE);
		IntWritable most_num = new IntWritable(Integer.MIN_VALUE);
		for (EarliestAndLastetLongWritable part : arg1) {
			//must use set method instead of =
			if (part.getStart_time().get()<earlist.get())
				earlist.set(part.getStart_time().get());
			if (part.getEnd_time().compareTo(lated) > 0)
				lated.set( part.getEnd_time().get());
			if (part.getLongest().get()>most_num.get())
				most_num.set(part.getLongest().get());
		}
		EarliestAndLastetLongWritable result = new EarliestAndLastetLongWritable();
		result.setEnd_time(lated);
		result.setStart_time(earlist);
		result.setLongest(most_num);
		arg2.write(arg0, result);
	}

}
