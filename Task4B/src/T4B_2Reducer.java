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


public class T4B_2Reducer extends TableReducer<IntWritable, ImmutableBytesWritable, ImmutableBytesWritable> {
	
	public void reduce(IntWritable key, Iterable<ImmutableBytesWritable> values, Context context) throws IOException, InterruptedException {
		
		//Create storage to sort list
		ArrayList<KeyValue> sortList = new ArrayList<KeyValue>();
		
		for(ImmutableBytesWritable v:values){
			//Break value back into articleID & count
			byte[] artid = Arrays.copyOfRange(v.get(), 0, 8);
			byte[] count = Arrays.copyOfRange(v.get(), 8, v.getLength());
			
			//Create new KeyValue to sort
			KeyValue sortItem = new KeyValue(Bytes.toLong(artid),Bytes.toInt(count));
			
			//Add to array list to sort
			sortList.add(sortItem);
		}
		
		//Perform sort
		Collections.sort(sortList, new KVCompare());
		
		//Emit Results
		for(int i=0;i<context.getConfiguration().getInt("display", 0);i++){
			Put put = new Put(Bytes.toBytes(sortList.get(i).getKey()));
			put.add(Bytes.toBytes("q4"),Bytes.toBytes("modify"), Bytes.toBytes(sortList.get(i).getValue()));
			//Write to DB
			context.write(null, put);
		}
	}
}
