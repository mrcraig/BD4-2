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


public class Task2B extends Configured implements Tool {
	
	public int run(String[] arg) throws Exception{
		//Set configuration
		Configuration conf = HBaseConfiguration.create(getConf());
		//conf.set("hbase.zookeeper.quorum", "bigdata-06.dcs.gla.ac.uk");
		conf.addResource("all-client-conf.xml");

		//Point to Jar
		conf.set("mapred.jar", "file:///users/level4/1002386c/Documents/BD4/AE2/Q2B.jar");
		
		//Setup job
		Job job = new Job(conf);
		job.setJarByClass(Task2B.class);
		
		//Set up Scanner
		//CF "WD" for sample dataset
		Scan scan = new Scan();
		scan.addColumn(Bytes.toBytes("q1"), Bytes.toBytes("in_artid"));
		scan.addColumn(Bytes.toBytes("q1"), Bytes.toBytes("in_revid"));
		scan.setCaching(100);
		scan.setCacheBlocks(false);
		
		job.getConfiguration().setStrings("daterange", arg[0],arg[1]);
		
		//Run Mapper
		TableMapReduceUtil.initTableMapperJob(
				"1002386c",
				scan,
				T2BMapper.class,
				ImmutableBytesWritable.class,
				ImmutableBytesWritable.class,
				job);
		
		TableMapReduceUtil.initTableReducerJob(
				"1002386c",
				T2BReducer.class,
				job);
		job.setNumReduceTasks(1);
		
		//Keep note of job status in order to return run status
		boolean status = job.waitForCompletion(true);
		
		//Read created table to output data to stdout
		
		HTable hTable = new HTable(conf, "1002386c");
		ResultScanner scanner = hTable.getScanner(Bytes.toBytes("q2"),Bytes.toBytes("revstr"));
		
		System.out.println("RESULTS:");
		for (Result res : scanner){
			System.out.println(Bytes.toLong(res.getRow()) + " " + Bytes.toString(res.value()));
		}
		
		//Delete data after output
		Delete delete = new Delete(Bytes.toBytes("b"));
		delete.deleteColumns(Bytes.toBytes("q2"), Bytes.toBytes("revstr"));
		hTable.delete(delete);
		
		
		scanner.close();
		hTable.close();
		
		return status ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception{
		System.exit(ToolRunner.run(new Task2B(), args));
	}
}
