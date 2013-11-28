import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;


public class T1Reducer extends TableReducer<ImmutableBytesWritable, LongWritable, ImmutableBytesWritable> {
	
	public void reduce(ImmutableBytesWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
		//Break current key into artID
		byte[] artID = Arrays.copyOfRange(key.get(), 0, 8);
		
		Put put = new Put(artID);
		
		for(LongWritable v:values){
			//Display on stdout since stupidly cannot read table
			System.out.println("K: " + key.get() + " V: " + v.get());
			
			//Emit KV pairs (should be already sorted)
			put.add(Bytes.toBytes("A"), key.get(), Bytes.toBytes(v.get()));
		}
	}
}
