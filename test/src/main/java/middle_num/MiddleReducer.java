package middle_num;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class MiddleReducer extends Reducer<IntWritable, IntWritable, IntWritable, NullWritable>{

	@Override
	protected void reduce(IntWritable arg0, Iterable<IntWritable> arg1,
			Reducer<IntWritable, IntWritable, IntWritable, NullWritable>.Context arg2)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		ArrayList<Integer> list = new ArrayList<Integer>();
		for(IntWritable temp:arg1) {
			list.add(temp.get());
		}
		list.sort(new Comparator<Integer>() {

			public int compare(Integer o1, Integer o2) {
				// TODO Auto-generated method stub
				if(o1>o2)
					return 1;
				else if(o1 == o2)
					return 0;
				else {
					return -1;
				}
			}
		});
		int middle_num = 0;
		if(list.size()%2==1) {
			middle_num = list.get((list.size()-1)/2);
		}else {
			middle_num = list.get(list.get((list.size()+1)/2)+list.get((list.size()+1)/2-1))/2;
		}
		arg2.write(new IntWritable(middle_num),null);	
	}

}
