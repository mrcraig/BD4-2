import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Task4B extends Configured implements Tool {
	
	public int run(String[] arg) throws Exception{
		//Set configuration
		Configuration conf = HBaseConfiguration.create(getConf());
		//conf.set("hbase.zookeeper.quorum", "bigdata-06.dcs.gla.ac.uk");
		conf.addResource("all-client-conf.xml");

		//Point to Jar
		conf.set("mapred.jar", "file:///users/level4/1002386c/Documents/BD4/AE2/Q4B.jar");
		
		//Setup job
		Job job = new Job(conf);
		job.setJarByClass(Task4B.class);
		
		//Set up Scanner
		//CF "WD" for sample dataset
		Scan scan = new Scan();
		scan.addColumn(Bytes.toBytes("q1"), Bytes.toBytes("in_artid"));
		scan.setCaching(100);
		scan.setCacheBlocks(false);
		
		job.getConfiguration().setStrings("daterange", arg[0],arg[1]);
		
		//Run Mapper
		TableMapReduceUtil.initTableMapperJob(
				"1002386c",
				scan,
				T4BMapper.class,
				ImmutableBytesWritable.class,
				IntWritable.class,
				job);
		
		TableMapReduceUtil.initTableReducerJob(
				"1002386c",
				T4BReducer.class,
				job);
		job.setNumReduceTasks(1);
		
		//Wait for first job to finish before starting second
		boolean status_1 = job.waitForCompletion(true);
		
		//Setup second job
		Job job_final = new Job(conf);
		job_final.setJarByClass(Task4B.class);
		job_final.getConfiguration().setInt("display", Integer.parseInt(arg[2]));
		
		//Add first-job defined columns to scan
		scan.addColumn(Bytes.toBytes("q4"), Bytes.toBytes("modify_int"));
		
		//Start second job mappers and reducers
		TableMapReduceUtil.initTableMapperJob(
				"1002386c",
				scan,
				T4B_2Mapper.class,
				IntWritable.class,
				ImmutableBytesWritable.class,
				job_final);
		
		TableMapReduceUtil.initTableReducerJob(
				"1002386c",
				T4B_2Reducer.class,
				job_final);
		job_final.setNumReduceTasks(1);
		
		
		//Keep note of job status in order to return run status
		boolean status = job_final.waitForCompletion(true);
		
		//Read created table to output data to stdout
		
		HTable hTable = new HTable(conf, "1002386c");
		ResultScanner scanner = hTable.getScanner(Bytes.toBytes("q4"),Bytes.toBytes("modify"));
		
		System.out.println("RESULTS:");
		for (Result res : scanner) {
			System.out.println(Bytes.toLong(res.getRow()) + " " + Bytes.toInt(res.value()));
		}
		
		//Delete data after output
		Delete delete = new Delete(Bytes.toBytes("b"));
		delete.deleteColumns(Bytes.toBytes("q4"), Bytes.toBytes("modify"));
		delete.deleteColumns(Bytes.toBytes("q4"),Bytes.toBytes("modify_int"));
		hTable.delete(delete);
		
		
		scanner.close();
		hTable.close();
		
		return (status && status_1) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception{
		System.exit(ToolRunner.run(new Task4B(), args));
	}
}
