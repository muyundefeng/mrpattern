package topN;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class TopNReducer extends Reducer<NullWritable, Text, Text, NullWritable>{
	
	public static TreeMap<Integer, String> map = new TreeMap<Integer, String>();
	@Override
	protected void reduce(NullWritable arg0, Iterable<Text> arg1,
			Reducer<NullWritable, Text, Text, NullWritable>.Context arg2) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		for(Text value:arg1) {
			String line = value.toString();
			int stop_num = Integer.parseInt(line.split(",")[4].replaceAll("\"",""));
			if(map.size()>10) {
				map.remove(map.firstKey());
			}else {
				map.put(stop_num, line);
			}
		}
		for(String value:map.values()) {
			arg2.write(new Text(value), NullWritable.get());
		}
	}

}
