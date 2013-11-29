import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


public class T1BMapper extends TableMapper<Text, LongWritable> {
	
	public void map(Text key, Result value, Context context) throws IOException, InterruptedException {
		
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
		Date ts = new Date(res[0].getTimestamp());
		byte[] artid = res[1].getValue();
		//Emit if falls within date range
		if(ts.after(timeStart) && ts.before(timeEnd)){
			//Break key to form seperate revid
			long revid = Bytes.toLong(res[2].getValue());
			
			//Output result
			context.write(new Text(artid), new LongWritable(revid));
		}
	}
}
