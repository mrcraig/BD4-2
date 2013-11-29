import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;


public class T4B_2Mapper extends TableMapper<IntWritable, ImmutableBytesWritable> {
	
	public void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
		
		
		//Produce concatenation of artID and count
		byte[] concat = Bytes.add(key.get(), value.value());
		
		//Write to reducer
		//Use artificial key to group all elements together
		context.write(new IntWritable(1), new ImmutableBytesWritable(concat));
	}
}
