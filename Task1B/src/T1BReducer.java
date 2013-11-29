import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


public class T1BReducer extends TableReducer<Text, LongWritable, ImmutableBytesWritable> {
	
	public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
		//Break current key into artID
		byte[] artID =(key.getBytes());
		
		Put put = new Put(artID);
		for(LongWritable v:values){
			//Emit KV pairs (should be already sorted)
			put.add(Bytes.toBytes("q1"), Bytes.toBytes("revid"), Bytes.toBytes(v.get()));
		}
		context.write(null, put);
	}
}

