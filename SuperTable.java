import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;

import org.apache.hadoop.hbase.TableName;

import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import org.apache.hadoop.hbase.util.Bytes;


public class SuperTable {
    private static final String TABLE_NAME = "powers";
    private static final String PERSONAL = "personal";
    private static final String PROFESSIONAL = "professional";

    public static void main(String[] args) throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        HBaseAdmin admin = new HBaseAdmin(configuration);

        HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
        descriptor.addFamily(new HColumnDescriptor(PERSONAL));
        descriptor.addFamily(new HColumnDescriptor(PROFESSIONAL));

        admin.createTable(descriptor);

        HTable table = new HTable(configuration, TABLE_NAME);
        table.put(makePut("row1", "superman", "strength", "clark", "100"));
        table.put(makePut("row2", "batman", "money", "bruce", "50"));
        table.put(makePut("row3", "wolverine", "healing", "logan", "75"));
        table.close();

        ResultScanner scanner = table.getScanner(Bytes.toBytes(PERSONAL), Bytes.toBytes("hero"));
        for (Result result: scanner) {
            System.out.println(result);
        }
        scanner.close();
    }

    private static Put makePut(String key, String hero, String power, String name, String xp) {
        Put put = new Put(Bytes.toBytes(key));
        put.add(Bytes.toBytes(PERSONAL), Bytes.toBytes("hero"), Bytes.toBytes(hero));
        put.add(Bytes.toBytes(PERSONAL), Bytes.toBytes("power"), Bytes.toBytes(power));
        put.add(Bytes.toBytes(PROFESSIONAL), Bytes.toBytes("name"), Bytes.toBytes(name));
        put.add(Bytes.toBytes(PROFESSIONAL), Bytes.toBytes("xp"), Bytes.toBytes(xp));
        return put;
    }
}
