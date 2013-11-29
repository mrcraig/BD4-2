import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Task2 extends Configured implements Tool {
	
	public int run(String[] arg) throws Exception{
		//Set configuration
		Configuration conf = HBaseConfiguration.create(getConf());
		//conf.set("hbase.zookeeper.quorum", "bigdata-06.dcs.gla.ac.uk");
		conf.addResource("all-client-conf.xml");

		//Point to Jar
		conf.set("mapred.jar", "file:///users/level4/1002386c/Documents/BD4/AE2/Q2A.jar");
		
		//Setup job
		Job job = new Job(conf);
		job.setJarByClass(Task2.class);
		
		//Set up Scanner
		//CF "WD" for sample dataset
		Scan scan = new Scan();
		//scan.addFamily(Bytes.toBytes("WD"));
		scan.setCaching(100);
		scan.setCacheBlocks(false);
		
		job.getConfiguration().setStrings("daterange", arg[0],arg[1]);
		
		//Run Mapper
		TableMapReduceUtil.initTableMapperJob(
				"BD4Project2Sample",
				scan,
				T2Mapper.class,
				ImmutableBytesWritable.class,
				ImmutableBytesWritable.class,
				job);
		
		TableMapReduceUtil.initTableReducerJob(
				"1002386c",
				T2Reducer.class,
				job);
		job.setNumReduceTasks(1);
		
		//Keep note of job status in order to return run status
		boolean status = job.waitForCompletion(true);
		
		//Read created table to output data to stdout
		
		HTable hTable = new HTable(conf, "1002386c");
		ResultScanner scanner = hTable.getScanner(Bytes.toBytes("q2"),Bytes.toBytes("revid"));
		
		System.out.println("RESULTS:");
		long current=0;
		int countrem=0;
		int countend=0;
		for (Result res : scanner) {
			//Remove key obfuscation
			//byte[] keyrem = Arrays.copyOfRange(res.getRow(), 0, 12);
			//set current artID
			long artid = Bytes.toLong(Arrays.copyOfRange(res.getRow(), 0, 12));
			if(current==artid && countrem>(countend+1)){
				//On same article, append to end of string
				System.out.print(" " + Bytes.toLong(res.value()));
				countend++;
			} else{
				// New article, yaaay!
				current = artid;
				//Parse artID and count
				byte[] count = Arrays.copyOfRange(res.getRow(), 12, res.getRow().length);
				countrem = Bytes.toInt(count);
				//Display first half of line
				System.out.print("\n" + artid + " " + Bytes.toInt(count) + " " + Bytes.toLong(res.value()));
			}
		}
		
		//Delete data after output
		Delete delete = new Delete(Bytes.toBytes("b"));
		delete.deleteColumns(Bytes.toBytes("q2"), Bytes.toBytes("revid"));
		hTable.delete(delete);
		
		
		scanner.close();
		hTable.close();
		
		return status ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception{
		System.exit(ToolRunner.run(new Task2(), args));
	}
}
