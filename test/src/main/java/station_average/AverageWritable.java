package station_average;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class AverageWritable implements Writable{

	private IntWritable count = new IntWritable();
	private FloatWritable temp = new FloatWritable();
	
	
	
	public IntWritable getCount() {
		return count;
	}

	public void setCount(IntWritable count) {
		this.count = count;
	}

	public FloatWritable getTemp() {
		return temp;
	}

	public void setTemp(FloatWritable temp) {
		this.temp = temp;
	}

	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		count = new IntWritable(arg0.readInt());
		temp = new FloatWritable(arg0.readFloat());
	}

	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		arg0.writeInt(count.get());
		arg0.writeFloat(temp.get());
	}

	@Override
	public String toString() {
		return "AverageWritable [count=" + count + ", temp=" + temp + "]";
	}

}
