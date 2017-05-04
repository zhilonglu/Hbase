package test1;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class hbaseCreate {
	static Configuration conf = HBaseConfiguration.create();
	static Connection conn = null;
	static{
		conf.set("hbase.rootdir", "hdfs://wyc-c3.test.com:8020/apps/hbase/data");//ʹ��eclipseʱ�����������������޷���λ
		conf.set("hbase.zookeeper.quorum", "wyc-a1.test.com,wyc-a2.test.com,wyc-b1.test.com");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("zookeeper.znode.parent","/hbase-unsecure");
	}
	//����һ���±�
	public static void createTable(String tableName, String[] columnFamilys){
		try {           
			conn = ConnectionFactory.createConnection(conf);
			System.out.println("StartConnect...");
			Admin hAdmin = conn.getAdmin();
			HTableDescriptor hTableDesc = new HTableDescriptor(TableName.valueOf(tableName));
			for (String columnFamily : columnFamilys) {
				hTableDesc.addFamily(new HColumnDescriptor(columnFamily));
			}
			hAdmin.createTable(hTableDesc);
			conn.close();
			System.out.println("Table created Successfully...");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	//ɾ�������
	public static void deleteTable(String tablename) throws IOException {
		try {
			conn = ConnectionFactory.createConnection(conf);
			Admin hAdmin = conn.getAdmin();
			hAdmin.disableTables(tablename);
			hAdmin.deleteTables(tablename);
			System.out.println("��ɾ���ɹ���");
			conn.close();
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		}
	}
	//����һ�м�¼   
	public static void getOneRecord (String tableName, String rowKey) throws IOException{       
		conn = ConnectionFactory.createConnection(conf);
		Table table_new = conn.getTable(TableName.valueOf(tableName));     
		Get get = new Get(rowKey.getBytes());       
		Result rs = table_new.get(get);       
		for(KeyValue kv : rs.raw()){       
			System.out.print(new String(kv.getRow()) + " " );       
			System.out.print(new String(kv.getFamily()) + ":" );       
			System.out.print(new String(kv.getQualifier()) + " " );       
			System.out.print(kv.getTimestamp() + " " );       
			System.out.println(new String(kv.getValue()));       
		}       
	}  
	//scanһ�ű�  startRow��stopRow���Ĳ���Ϊnullʱ���Զ�ɨȫ��
	public static void getScanRecord(String tablename,String startRow,String stopRow)
	{
		try {
			conn = ConnectionFactory.createConnection(conf);
			Table table_new = conn.getTable(TableName.valueOf(tablename));
			ResultScanner rs = null;  
			Scan scan = new Scan();  
			if (startRow != null) { // ����ɨ��ķ�Χ  
				scan.setStartRow(Bytes.toBytes(startRow));  
			}  
			if (stopRow != null) {  
				scan.setStopRow(Bytes.toBytes(stopRow));  
			}
			rs = table_new.getScanner(scan);  
			table_new.close();
			for (Result r : rs) {// ����ȥ����  
				for (KeyValue kv : r.raw()) {// ����ÿһ�еĸ���  
					StringBuffer sb = new StringBuffer()  
					.append(Bytes.toString(kv.getRow())).append("\t")  
					.append(Bytes.toString(kv.getFamily()))  
					.append("\t")  
					.append(Bytes.toString(kv.getQualifier()))  
					.append("\t").append(Bytes.toString(kv.getValue()));  
					System.out.println(sb.toString());  
				}  
			}  
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	//���������
	public static void main(String[] args){
		String tableName = "test";
		//scan Hbase�������
		getScanRecord(tableName,null,null);
		String[] columnFamilys = { "info"};
		createTable(tableName,columnFamilys);
		try {
			Date startTime = new Date(System.currentTimeMillis());
			SimpleDateFormat sdFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			System.out.println("Start add.......");
			System.out.println(sdFormatter.format(startTime));
			conn = ConnectionFactory.createConnection(conf);
			Table table_new = conn.getTable(TableName.valueOf(tableName));
			List<Put> putslist = null;
			int count=0;
			for(int i=0;i<1000;i++){
				putslist = new LinkedList<>();
				String rowkey = String.valueOf(i);
				Put put = new Put(rowkey.getBytes());
				//���ִ洢��ʽ��Ч���ر�ͣ��洢�ٶ��ر���
				put.addColumn("info".getBytes(), "age".getBytes(), "20".getBytes());
				put.addColumn("info".getBytes(), "sex".getBytes(), "male".getBytes());
				put.addColumn("info".getBytes(), "name".getBytes(), "Jame".getBytes());
				put.addColumn("info".getBytes(), "career".getBytes(), "student".getBytes());
				put.addColumn("info".getBytes(), "home".getBytes(), "china".getBytes());
				putslist.add(put);
				count++;
				table_new.put(putslist);
			}
			table_new.close();
			Date stopTime = new Date(System.currentTimeMillis());
			System.out.println("Stop add.......");
			System.out.println(sdFormatter.format(stopTime));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
