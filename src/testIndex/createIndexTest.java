package testIndex;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
public class createIndexTest {
    //map阶段，根据hbase中的数据取出行健和姓名
    public static class HbaseIndexMapper extends TableMapper<ImmutableBytesWritable, ImmutableBytesWritable>{
        @Override
        protected void map(ImmutableBytesWritable key, Result value,
                Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, ImmutableBytesWritable>.Context context)
                        throws IOException, InterruptedException {
            List<Cell> cs = value.listCells();
            for (Cell cell : cs) {
                String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                System.out.println("qualifier="+qualifier);
                if(qualifier.equals("name")){
                    //把名字做键  行健做值输出
                    context.write(new ImmutableBytesWritable(CellUtil.cloneValue(cell)), new ImmutableBytesWritable(CellUtil.cloneRow(cell)));
                }
            }
        }
    }
    //reduce阶段，将姓名作为键，行健作为值存入hbase
    public static class HbaseIndexReduce extends TableReducer<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable>{
        @Override
        protected void reduce(ImmutableBytesWritable key, Iterable<ImmutableBytesWritable> value,
                Reducer<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable, Mutation>.Context context)
                        throws IOException, InterruptedException {
            //把名字做行健
            Put put=new Put(key.get());
            //把行健做值
            for (ImmutableBytesWritable v : value) {
                put.addColumn("rowkey".getBytes(),"index".getBytes(),v.get() );
            }
            context.write(key, put);
        }
    }
    private static void checkTable(Configuration conf) throws Exception {
        Connection con = ConnectionFactory.createConnection(conf);
        Admin admin = con.getAdmin();
        TableName tn = TableName.valueOf("heroesIndex");
        if (!admin.tableExists(tn)){
            HTableDescriptor htd = new HTableDescriptor(tn);
            HColumnDescriptor hcd = new HColumnDescriptor("rowkey".getBytes());
            htd.addFamily(hcd);
            admin.createTable(htd);
            System.out.println("表不存在，新创建表成功....");
        }
    }
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            conf = HBaseConfiguration.create(conf);
            conf.set("hbase.rootdir", "hdfs://wyc-c3.test.com:8020/apps/hbase/data");//使用eclipse时必须添加这个，否则无法定位
            conf.set("hbase.zookeeper.quorum", "wyc-a1.test.com,wyc-a2.test.com,wyc-b1.test.com");
    		conf.set("hbase.zookeeper.property.clientPort", "2181");
    		conf.set("zookeeper.znode.parent","/hbase-unsecure");
            Job job = Job.getInstance(conf, "heroes");
            job.setJarByClass(createIndexTest.class);
            Scan scan = new Scan();
            scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"));
            TableMapReduceUtil.initTableMapperJob("heroes", scan, HbaseIndexMapper.class, 
                    ImmutableBytesWritable.class, ImmutableBytesWritable.class,job);
            TableMapReduceUtil.initTableReducerJob("heroesIndex", HbaseIndexReduce.class, job);
            checkTable(conf);
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}