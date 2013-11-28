import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseScannerTest {
	private static final int LIMIT = 10;
	public static void main(String[] args) throws IOException {
		Configuration conf = HBaseConfiguration.create();
		conf.addResource("all-client-conf.xml");

		int numRows = LIMIT;
		if (args.length == 1)
			numRows = Integer.parseInt(args[0], numRows);
		if (numRows < 0)
			System.exit(1);

		HTable hTable = new HTable(conf, "BD4Project2Sample");
		ResultScanner scanner = hTable.getScanner(Bytes.toBytes("WD"));

		for (Result res : scanner) {
			System.out.println(res);
			if (--numRows == 0)
				break;
		}
		scanner.close();
		hTable.close();
		System.exit(0);
	}
}
