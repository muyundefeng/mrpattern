package bus_data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

public class EarliestAndLastetLongWritable implements Writable{
	
	private LongWritable start_time = new LongWritable();
	private LongWritable end_time = new LongWritable();
	private IntWritable longest  = new IntWritable();

	
	public LongWritable getStart_time() {
		return start_time;
	}

	public void setStart_time(LongWritable start_time) {
		this.start_time = start_time;
	}

	public LongWritable getEnd_time() {
		return end_time;
	}

	public void setEnd_time(LongWritable end_time) {
		this.end_time = end_time;
	}

	public IntWritable getLongest() {
		return longest;
	}

	public void setLongest(IntWritable longest) {
		this.longest = longest;
	}

	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		start_time = new LongWritable(arg0.readLong());
		end_time = new LongWritable(arg0.readLong());
		longest = new IntWritable(arg0.readInt());
//		start_time.readFields(arg0);
//		end_time.readFields(arg0);
//		longest.readFields(arg0);
	}

	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		arg0.writeLong(start_time.get());
		arg0.writeLong(end_time.get());
		arg0.writeInt(longest.get());
//		start_time.write(arg0);
//		end_time.write(arg0);
//		longest.write(arg0);
	}

	@Override
	public String toString() {
		return "EarliestAndLastetLongWritable [start_time=" + start_time + ", end_time=" + end_time + ", longest="
				+ longest + "]";
	}
	

}
