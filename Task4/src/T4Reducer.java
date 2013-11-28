import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;


public class T4Reducer extends TableReducer<ImmutableBytesWritable, IntWritable, ImmutableBytesWritable> {
	
	public void reduce(ImmutableBytesWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		//Break current key into artID
		
		int count=0;
		for(IntWritable v:values){
			count++;
		}
		
		int noMods = context.getConfiguration().getInt("modifications", 0);
		
		Put put = new Put(key.get());
		
		
		if(count>=noMods){
			System.out.println("key: " + Bytes.toLong(key.get()) + " count: " + count);
			//Emit artID & number of times modified
			put.add(Bytes.toBytes("q3"),Bytes.toBytes("modify"),Bytes.toBytes(count));
			//Write to database
			context.write(null, put);
		}
	}
}
