package testIndex; 
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class createTable {
    public static void main(String[] args) {
        Admin admin = null;
        Connection con = null;
        try {
            Configuration conf = HBaseConfiguration.create(); // 获得配制文件对象
            conf.set("hbase.rootdir", "hdfs://wyc-c3.test.com:8020/apps/hbase/data");//使用eclipse时必须添加这个，否则无法定位
            conf.set("hbase.zookeeper.quorum", "wyc-a1.test.com,wyc-a2.test.com,wyc-b1.test.com");
    		conf.set("hbase.zookeeper.property.clientPort", "2181");
    		conf.set("zookeeper.znode.parent","/hbase-unsecure");
            con = ConnectionFactory.createConnection(conf); // 获得连接对象
            TableName tn = TableName.valueOf("heroes");
            admin = con.getAdmin();
            HTableDescriptor htd = new HTableDescriptor(tn);
            HColumnDescriptor hcd = new HColumnDescriptor("info");
            htd.addFamily(hcd);
            admin.createTable(htd);
            admin.close();
            con = ConnectionFactory.createConnection(conf); // 获得连接对象
            Table t = con.getTable(tn);
            String[] heronames = new String[] { "peter", "hiro", "sylar", "claire", "noah" };
            for (int i = 0; i < 5; i++) {
            	System.out.println(i);
                Put put = new Put(String.valueOf(i).getBytes());
                put.addColumn("info".getBytes(), "name".getBytes(), heronames[i].getBytes());
                put.addColumn("info".getBytes(), "email".getBytes(), String.valueOf((i + "@qq.com")).getBytes());
                put.addColumn("info".getBytes(), "power".getBytes(), "Idotknow".getBytes());
                System.out.println(i);
                t.put(put);
            }
            con.close();
            t.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}