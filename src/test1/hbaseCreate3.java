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
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

public class hbaseCreate3 {
	static Configuration conf = HBaseConfiguration.create();
	static Connection conn = null;
	static{
		conf.set("hbase.rootdir", "hdfs://redis1.hhdata.com:8020/apps/hbase/data");//使用eclipse时必须添加这个，否则无法定位
		conf.set("hbase.zookeeper.quorum", "redis1.hhdata.com,redis2.hhdata.com,sql1.hhdata.com");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("zookeeper.znode.parent","/hbase-unsecure");
	}
	//插入一条记录
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
	//删除表操作
	public static void deleteTable(String tablename) throws IOException {
		try {
			conn = ConnectionFactory.createConnection(conf);
			Admin hAdmin = conn.getAdmin();
			hAdmin.disableTables(tablename);
			hAdmin.deleteTables(tablename);
			System.out.println("表删除成功！");
			conn.close();
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		}
	}
	//主程序入口    存储数据的话以该程序为主
	public static void main(String[] args){
		String tableName = "test0913";
		String[] columnFamilys = { "info"};
		createTable(tableName,columnFamilys);
		try {
			Date startTime = new Date(System.currentTimeMillis());
			SimpleDateFormat sdFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			System.out.println("Start add.......");
			System.out.println(sdFormatter.format(startTime));
			conn = ConnectionFactory.createConnection(conf);
			Table table_new = conn.getTable(TableName.valueOf(tableName));
			//按list进行put的话这样的效率最高，存储的速度最快
			int count=0;
			List<Put> putslist = null;
			for(int i=0;i<100000;i++){
				if(i%100000==0){//这里的100000是个参数，可以调整。值的大小可以调整插入数据的速度
					System.out.println(i);
					System.out.println(new Date(System.currentTimeMillis()));
					putslist = new LinkedList<>();
				}
				String rowkey = String.valueOf(i);
				Put put = new Put(rowkey.getBytes());
				put.addColumn("info".getBytes(), "age".getBytes(), "20".getBytes());
				put.addColumn("info".getBytes(), "sex".getBytes(), "male".getBytes());
				put.addColumn("info".getBytes(), "name".getBytes(), "Jame".getBytes());
				put.addColumn("info".getBytes(), "career".getBytes(), "student".getBytes());
				put.addColumn("info".getBytes(), "home".getBytes(), "china".getBytes());
				putslist.add(put);
				count++;
				if(count%100000==0){
					table_new.put(putslist);
				}
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
