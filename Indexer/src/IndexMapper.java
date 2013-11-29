import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;


public class IndexMapper extends TableMapper<ImmutableBytesWritable, LongWritable> {
	
	public void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
		
		//For each value, convert to KV and get timestamp
		KeyValue[] res = value.raw();
		
		//Get timestamp
		Date ts = new Date(res[0].getTimestamp());
		
		//Emit artid&revid with timestamp
		context.write(new ImmutableBytesWritable(key.get()), new LongWritable(ts.getTime()));
	}
}
