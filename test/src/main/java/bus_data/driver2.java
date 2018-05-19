package bus_data;

import java.io.FileOutputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class driver2 {
	public static void main(String[] args) throws Exception {
		  String dst = "hdfs://nj01-nanling-hdfs.dmop.baidu.com:54310/app/ps/spider/Pie/data-plat/pie/35281/task_4867_pie_manual_35281_2018-05-15T114000.58440/pack";  
		  Configuration conf = new Configuration();  
		  FileSystem fs = FileSystem.get(URI.create(dst), conf);
		  FSDataInputStream hdfsInStream = fs.open(new Path(dst));
		  
		  OutputStream out = new FileOutputStream("/Users/lisheng05/work/jd_jiqun/a.txt"); 
		  byte[] ioBuffer = new byte[1024];
		  int readLen = hdfsInStream.read(ioBuffer);

		  while(-1 != readLen){
			  out.write(ioBuffer, 0, readLen);  
			  readLen = hdfsInStream.read(ioBuffer);
		  }
		  out.close();
		  hdfsInStream.close();
		  fs.close();	
		  }
}
