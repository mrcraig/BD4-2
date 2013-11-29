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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;


public class T2BMapper extends TableMapper<ImmutableBytesWritable, ImmutableBytesWritable> {
	
	public void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
		
		//Import date values from context
		Date timeStart=null;
		Date timeEnd=null;
		
		//Parse inputted date range into something system can understand
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
		
		try{
			timeStart = dateFormat.parse(context.getConfiguration().getStrings("daterange")[0]);
			timeEnd = dateFormat.parse(context.getConfiguration().getStrings("daterange")[1]);
		} catch (ParseException e) {
			System.out.println("Could not parse date");
		}
		
		//For each value, convert to KV and get timestamp
		KeyValue[] res = value.raw();
		//Treat timestamp as a date
		Date ts = new Date(Bytes.toLong(key.get()));
		
		//Emit if falls within date range
		if(ts.after(timeStart) && ts.before(timeEnd)){
			//Parse artid & revid
			KeyValue artid = value.getColumnLatest(Bytes.toBytes("q1"),Bytes.toBytes("in_artid"));
			KeyValue revid = value.getColumnLatest(Bytes.toBytes("q1"),Bytes.toBytes("in_revid"));
			
			//Emit 1 for each article ID
			context.write(new ImmutableBytesWritable(artid.getValue()), new ImmutableBytesWritable(revid.getValue()));
		}
	}
}
