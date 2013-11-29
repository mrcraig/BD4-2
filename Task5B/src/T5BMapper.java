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


public class T5BMapper extends TableMapper<ImmutableBytesWritable, ImmutableBytesWritable> {
	
	public void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
		
		//Import date values from context
		Date timeDesired=null;
		
		//Parse inputted date range into something system can understand
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
		
		try{
			timeDesired = dateFormat.parse(context.getConfiguration().getStrings("date")[0]);
		} catch (ParseException e) {
			System.out.println("Could not parse date");
		}
		
		//Get timestamp for comparison
		Date ts = new Date(Bytes.toLong(key.get()));
		
		//Emit if falls timestamp was created at that point in time
		if(timeDesired.after(ts)){
			//Parse key into artID & revID
			KeyValue artid = value.getColumnLatest(Bytes.toBytes("q1"),Bytes.toBytes("in_artid"));
			KeyValue revid = value.getColumnLatest(Bytes.toBytes("q1"),Bytes.toBytes("in_revid"));
			
			//Concatenate revid & timestamp to pass to reducer
			byte[] concat = Bytes.add(revid.getValue(), Bytes.toBytes(ts.getTime()));
			
			//Emit 1 for each article ID
			context.write(new ImmutableBytesWritable(artid.getValue()), new ImmutableBytesWritable(concat));
		}
	}
}
