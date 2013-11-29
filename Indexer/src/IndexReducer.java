import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;


public class IndexReducer extends TableReducer<ImmutableBytesWritable, LongWritable, ImmutableBytesWritable> {
	
	public void reduce(ImmutableBytesWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
		//Select table
		HTable hTable = new HTable(context.getConfiguration(),"1002386c");
		
		//Parse artID & revID
		byte[] artID = Arrays.copyOfRange(key.get(), 0, 8);
		byte[] revID = Arrays.copyOfRange(key.get(), 8, key.getLength());
		Put put = null;
		
		for(LongWritable v:values){
			//Use timestamp as key
			put = new Put(Bytes.toBytes(v.get()));
		}
		
		/*
		 * Column Names
		 * in_artid : article id
		 * in_revid : revision id
		 */
		put.add(Bytes.toBytes("q1"), Bytes.toBytes("in_artid"), artID);
		put.add(Bytes.toBytes("q1"), Bytes.toBytes("in_revid"), revID);
		context.write(null, put);
	}
}
