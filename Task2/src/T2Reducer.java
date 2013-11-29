import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;


public class T2Reducer extends TableReducer<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable> {
	
	public void reduce(ImmutableBytesWritable key, Iterable<ImmutableBytesWritable> values, Context context) throws IOException, InterruptedException {
		
		//Declare array storage to store revisions
		ArrayList<byte[]> revlist = new ArrayList<byte[]>();
		
		for(ImmutableBytesWritable v:values){
			revlist.add(v.get());
		}
		
		//Output each revision id for pk
		String revstr = "" + revlist.size();
		for(byte[] b:revlist){
			//append each revid to string
			revstr = revstr + " " + Bytes.toLong(b);
		}
		
		Put put = new Put(key.get());
		put.add(Bytes.toBytes("q2"),Bytes.toBytes("revstr"),Bytes.toBytes(revstr));
		
		//Add to DB
		context.write(null, put);
		
	}
}
