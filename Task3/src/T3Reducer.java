import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;


public class T3Reducer extends TableReducer<ImmutableBytesWritable, LongWritable, ImmutableBytesWritable> {
	
	public void reduce(ImmutableBytesWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
		//Break current key into artID
		byte[] artID = Arrays.copyOfRange(key.get(), 0, 8);
		HTable hTable = new HTable(context.getConfiguration(),"1002386c");
		
		Put put = new Put(artID);
		for(LongWritable v:values){
			//Emit KV pairs (should be already sorted)
			System.out.println(Bytes.toLong(artID) + "--" + v.get());
			put.add(Bytes.toBytes("q3"), Bytes.toBytes("revid"), Bytes.toBytes(v.get()));
		}
		context.write(null, put);
	}
}
