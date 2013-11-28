import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Task1 extends Configured implements Tool {
	
	public int run(String[] arg) throws Exception{
		//Set configuration
		Configuration conf = HBaseConfiguration.create(getConf());
		//conf.set("hbase.zookeeper.quorum", "bigdata-06.dcs.gla.ac.uk");
		conf.addResource("all-client-conf.xml");

		//Point to Jar
		conf.set("mapred.jar", "file:///users/level4/1002386c/Documents/BD4/AE2/Q1A.jar");
		
		//Setup job
		Job job = new Job(conf);
		job.setJarByClass(Task1.class);
		
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
				T1Mapper.class,
				ImmutableBytesWritable.class,
				LongWritable.class,
				job);
		
		TableMapReduceUtil.initTableReducerJob(
				"1002386c",
				T1Reducer.class,
				job);
		job.setNumReduceTasks(1);
		
		//Keep note of job status in order to return run status
		boolean status = job.waitForCompletion(true);
		
		//Read created table to output data to stdout
		
		HTable hTable = new HTable(conf, "1002386c");
		ResultScanner scanner = hTable.getScanner(Bytes.toBytes("q1"));

		System.out.println("RESULTS:");
		for (Result res : scanner) {
			System.out.println("artID: " + Bytes.toLong(res.getRow()) + " revid: " + Bytes.toLong(res.value()));
		}
		scanner.close();
		hTable.close();
		
		return status ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception{
		System.exit(ToolRunner.run(new Task1(), args));
	}
}
