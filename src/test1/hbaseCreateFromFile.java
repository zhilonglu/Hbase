package test1;
/*
 * 通过读取批量写入Hbase
 * 
 */
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import net.sf.json.JSONObject;

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

public class hbaseCreateFromFile {
	static Configuration conf = HBaseConfiguration.create();
	static Connection conn = null;
	static SimpleDateFormat sdFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	static{
		conf.set("hbase.rootdir", "hdfs://wyc-c3.test.com:8020/apps/hbase/data");//使用eclipse时必须添加这个，否则无法定位
		conf.set("hbase.zookeeper.quorum", "wyc-a1.test.com,wyc-a2.test.com,wyc-b1.test.com");
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
	//读取文件写入Hbase
	public static void readFromFile(String pathname,Table table_new)
	{
		File rootfile = new File(pathname);
		File[] files1 = rootfile.listFiles();
		BufferedReader br = null;
		String record;
		int cnt = 0;
		List<Put> putslist = null;
		for(int i=0;i<files1.length;i++){
			try {
				br = new BufferedReader(new FileReader(files1[i]));
				try {
					while((record=br.readLine())!=null){
						if(record.contains("{")&&record.contains("}")){//合法的json文件进行解析
							if(cnt%500000==0){
								putslist = new LinkedList<>();
							}
							JSONObject js = JSONObject.fromObject(record);
							String source = js.getString("source");
							String driverName = js.getString("dirverName");
							String driverIdCode = js.getString("dirverIdCode");
							String driverTcode = js.getString("driverTcode");
							String vehType = js.getString("vehType");
							String vehNo = js.getString("vehNo");
							String gpsTime = js.getString("gpsTime");
							String lon = js.getString("lon");
							String lat = js.getString("lat");
							String velocity = js.getString("velocity");
							String vehStatus = js.getString("vehStatus");
							//设置key值为车牌号+时间，中间是#分隔
							String rkey = vehNo+"#"+gpsTime;
							Put put = new Put(rkey.getBytes());
							put.addColumn("info".getBytes(), "source".getBytes(), source.getBytes());
							put.addColumn("info".getBytes(), "driverName".getBytes(), driverName.getBytes());
							put.addColumn("info".getBytes(), "driverIdCode".getBytes(), driverIdCode.getBytes());
							put.addColumn("info".getBytes(), "driverTcode".getBytes(), driverTcode.getBytes());
							put.addColumn("info".getBytes(), "vehType".getBytes(), vehType.getBytes());
							put.addColumn("info".getBytes(), "lon".getBytes(), lon.getBytes());
							put.addColumn("info".getBytes(), "lat".getBytes(), lat.getBytes());
							put.addColumn("info".getBytes(), "velocity".getBytes(), velocity.getBytes());
							put.addColumn("info".getBytes(), "vehStatus".getBytes(), vehStatus.getBytes());
							putslist.add(put);
							cnt++;
							if(cnt%500000==0){
								System.out.println(sdFormatter.format(new Date(System.currentTimeMillis()))+"#"+cnt/500000);
								table_new.put(putslist);
							}
						}
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		try {
			System.out.println("总共写入行数:--------->"+cnt);
			table_new.put(putslist);
			table_new.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}//将表关闭
	}
	//主程序入口    存储数据的话以该程序为主
	public static void main(String[] args){
		if (args.length < 2) {
			System.err.println("Usage:<sourcePath> <tableName>");
			System.exit(2);
		}
		String tableName = args[1];
		try {
			Date startTime = new Date(System.currentTimeMillis());
			
			System.out.println("Start add.......");
			System.out.println(sdFormatter.format(startTime));
			conn = ConnectionFactory.createConnection(conf);
			Table table_new = conn.getTable(TableName.valueOf(tableName));
			readFromFile(args[0],table_new);
			Date stopTime = new Date(System.currentTimeMillis());
			System.out.println("Stop add.......");
			System.out.println(sdFormatter.format(stopTime));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
