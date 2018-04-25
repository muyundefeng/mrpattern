package middle_num;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
/*
 * 计算每条公交线路的平均站数，保留小数
 */

public class MiddleDriver {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "middler");
		job.setJarByClass(MiddleDriver.class);
		//mapper与reducer的输出类型不一样，需要单独指定mapper的输出类型
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(NullWritable.class);
			
		job.setMapperClass(MiddleMapper.class);
//		job.setCombinerClass(AverageReducer.class);
		job.setReducerClass(MiddleReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}
