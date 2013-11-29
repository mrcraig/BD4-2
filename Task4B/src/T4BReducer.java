import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;


public class T4BReducer extends TableReducer<ImmutableBytesWritable, IntWritable, ImmutableBytesWritable> {
	
	public void reduce(ImmutableBytesWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		//Break current key into artID
		
		int count=0;
		for(IntWritable v:values){
			count++;
		}

		//Store artID as key and number modifications as value
		Put put = new Put(key.get());
		put.add(Bytes.toBytes("q4"),Bytes.toBytes("modify_int"),Bytes.toBytes(count));
		//Write to database
		context.write(null, put);
		
	}
}
