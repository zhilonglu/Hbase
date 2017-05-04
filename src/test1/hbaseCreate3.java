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
		conf.set("hbase.rootdir", "hdfs://redis1.hhdata.com:8020/apps/hbase/data");//ʹ��eclipseʱ�����������������޷���λ
		conf.set("hbase.zookeeper.quorum", "redis1.hhdata.com,redis2.hhdata.com,sql1.hhdata.com");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("zookeeper.znode.parent","/hbase-unsecure");
	}
	//����һ����¼
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
	//���������    �洢���ݵĻ��Ըó���Ϊ��
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
			//��list����put�Ļ�������Ч����ߣ��洢���ٶ����
			int count=0;
			List<Put> putslist = null;
			for(int i=0;i<100000;i++){
				if(i%100000==0){//�����100000�Ǹ����������Ե�����ֵ�Ĵ�С���Ե����������ݵ��ٶ�
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
