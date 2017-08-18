import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * Created by wangchao on 2017/6/26.
 */
public class ConnectTest {
    static Configuration conf = null;
    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "namenode,datanode1,datanode2");

    }

    @Test
    public void connect(){
        try {
            Connection connection = ConnectionFactory.createConnection(conf);
            Table t1 = connection.getTable(TableName.valueOf("t1"));
            Get get = new Get(Bytes.toBytes("r1"));
            Result result = t1.get(get);
            byte[] value = result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("c10"));
            System.out.println(new String(value));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void putTest(){
        try {
            Connection connection = ConnectionFactory.createConnection(conf);
            Table t1 = connection.getTable(TableName.valueOf("t1"));
            Put put = new Put(Bytes.toBytes("r1"));
//            Cell cell = CellUtil.createCell(Bytes.toBytes("f1"), Bytes.toBytes("c10"), Bytes.toBytes("chao"));
            put.add(Bytes.toBytes("f1"), Bytes.toBytes("c10"), Bytes.toBytes("chao1"));
            t1.put(put);
            t1.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    @Test
    public void regionLocateTest(){
        try {
            Connection connection = ConnectionFactory.createConnection(conf);
            RegionLocator t1 = connection.getRegionLocator(TableName.valueOf("t1"));
            List<HRegionLocation> allRegionLocations = t1.getAllRegionLocations();
            //iter 增强for循环快捷键
            //打印t1表的所有region所在的hostname和regioninfo
            for (HRegionLocation regionLocation : allRegionLocations) {
                System.out.println(regionLocation.getHostname());
                System.out.println("region:"+regionLocation.getRegionInfo());
            }
            HRegionLocation r1 = t1.getRegionLocation(Bytes.toBytes("r1"));
            System.out.println("r1:"+r1.getHostname()+"\t"+r1.getRegionInfo());
            Pair<byte[][], byte[][]> startEndKeys = t1.getStartEndKeys();
            System.out.println("first:"+startEndKeys.getFirst().toString());
            System.out.println("second:"+startEndKeys.getSecond().toString());



        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    @Test
    public void testScan(){
        Connection connection = null;
        try {
            connection = ConnectionFactory.createConnection(conf);
            Table t1 = connection.getTable(TableName.valueOf("t1"));
            Scan scan = new Scan();
            ResultScanner scanner = t1.getScanner(scan);
            for (Result result : scanner) {
                Cell[] cells = result.rawCells();
                for (Cell cell : cells) {
                    System.out.println("family:" + Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()));
                    System.out.println("qualifier:" + Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()));
                    System.out.println("value:" + Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                    System.out.println("Timestamp:" + cell.getTimestamp());
                    System.out.println("--------------------------");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}