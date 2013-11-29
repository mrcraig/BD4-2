import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;


public class T5Reducer extends TableReducer<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable> {
	
	public void reduce(ImmutableBytesWritable key, Iterable<ImmutableBytesWritable> values, Context context) throws IOException, InterruptedException {
		
		//Declare storage for sorting
		ArrayList<RevTime> sortList = new ArrayList<RevTime>();
		
		//For each article ID, add revision ID & timestamp to list to sort
		for(ImmutableBytesWritable v:values){
			//Split value into revid & timestamp
			long revid = Bytes.toLong(Arrays.copyOfRange(v.get(), 0, 8));
			long ts = Bytes.toLong(Arrays.copyOfRange(v.get(), 8, v.getLength()));
			
			//Add a new RevTime to comparison list
			RevTime toCompare = new RevTime(revid,ts);
			sortList.add(toCompare);
		}
		
		//When all revisions for article ID exhausted, sort list
		Collections.sort(sortList,new RTCompare());
		
		//Output top element as the most recent
		//Use article ID as key to sort list
		Put put = new Put(key.get());
		byte[] concat = Bytes.add(Bytes.toBytes(sortList.get(0).getRev()), Bytes.toBytes(sortList.get(0).getTs()));
		put.add(Bytes.toBytes("q5"), Bytes.toBytes("revidts"), concat);
		
		//Write to DB
		context.write(null,put);
	}
}
