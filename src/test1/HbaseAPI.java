package test1;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseAPI {
	public static Configuration conf = HBaseConfiguration.create();
	public static Connection conn = null;
	static{
		conf.set("hbase.rootdir", "hdfs://wyc-c3.test.com:8020/apps/hbase/data");//ʹ��eclipseʱ�����������������޷���λ
		conf.set("hbase.zookeeper.quorum", "wyc-a1.test.com,wyc-a2.test.com,wyc-b1.test.com");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("zookeeper.znode.parent","/hbase-unsecure");
	}
	//countͳ��Hbase����
	public static long rowCount(String tableName) {  
		long rowCount = 0;  
		try {  
			HTable table = new HTable(conf, tableName);  
			Scan scan = new Scan();  
			scan.setCaching(500);  
			scan.setCacheBlocks(false);
			scan.setFilter(new FirstKeyOnlyFilter());  
			ResultScanner resultScanner = table.getScanner(scan);  
			for (Result result : resultScanner) {  
				rowCount += result.size();  
				if(rowCount%100000==0)
					System.out.println(rowCount);
			}  
		} catch (IOException e) {  
		}  
		return rowCount;  
	} 
	//Hbaseͨ��api��ȡһ��ֵ
	public static void getDataTest(String tableName){
		System.out.println(System.currentTimeMillis());
		Table table_new;
		try {
			table_new = conn.getTable(TableName.valueOf(tableName));
			Get get = new Get("967790028738#2016-04-02 06:18:09".getBytes());       
			Result rs = table_new.get(get);       
			for(KeyValue kv : rs.raw()){       
				//				System.out.print(new String(kv.getRow()) + " " );       
				//				System.out.print(new String(kv.getFamily()) + ":" );       
				//				System.out.print(new String(kv.getQualifier()) + " " );       
				//				System.out.print(kv.getTimestamp() + " " );       
				//				System.out.println(new String(kv.getValue()));       
			} 
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}     
		System.out.println(System.currentTimeMillis());
	}
	//hbaseͨ��api����startrowkey��endrowkey���в�ѯ��
	public static long scanTest(String tableName){
		long rowCount = 0;  
				String startKey = "967790028738";
				String stopKey =  "967790028739";//stoprow���ᱻ���ǽ�ȥ
//		String startKey = "1";
//		String stopKey =  "6";//stoprow���ᱻ���ǽ�ȥ
		SimpleDateFormat sdFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date tempTime = new Date(System.currentTimeMillis());
		System.out.println(sdFormatter.format(tempTime));
		try {  
			HTable table = new HTable(conf, tableName);  
			Scan scan = new Scan();  
			scan.setCaching(3000);  
			scan.setCacheBlocks(false);
			scan.setStartRow(startKey.getBytes());
			scan.setStopRow(stopKey.getBytes());
			//			scan.setFilter(new FirstKeyOnlyFilter());//���ù�������ֻ��ʾ��һ��
			ResultScanner resultScanner = table.getScanner(scan);  
			for (Result result : resultScanner) {  
				rowCount += 1; 
//				rowCount += result.size();
				for (KeyValue kv : result.raw()) {// ����ÿһ�еĸ���
					StringBuffer sb = new StringBuffer()  
					.append(Bytes.toString(kv.getRow())).append("\t")  
					.append(Bytes.toString(kv.getFamily()))  
					.append("\t")  
					.append(Bytes.toString(kv.getQualifier()))  
					.append("\t").append(Bytes.toString(kv.getValue()));  
					//					System.out.println(sb.toString());
				}
			}  
		}catch (IOException e) {  
		}  
		tempTime = new Date(System.currentTimeMillis());
		System.out.println(sdFormatter.format(tempTime));
		return rowCount; 
	}
	public static void main(String[] args){
		try {
			conn = ConnectionFactory.createConnection(conf);
			//System.out.println(rowCount("gpsDataTest"));
			System.out.println(scanTest("gpsDataTest2"));
//			getDataTest("gpsDataTest2");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
