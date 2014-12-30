package org.sandcastle.learnings.mapr;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import java.io.IOException;

/**
 * Class uploads Student Model data into HBase setup in configured location on server side.
 *
 * @version 1.0
 */
public class DataUploadDemo {
    public static final Logger log = LoggerFactory.getLogger(DataUploadDemo.class);

    private static final String DATA_TABLE_STORE = "/usr/root/students";

    public static void main(String[] args) throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        HTable table = new HTable(configuration, DATA_TABLE_STORE);
        Put handler = new Put("student".getBytes());

        byte[] account = "account".getBytes();
        byte[] address = "address".getBytes();

        handler.add(account, "name".getBytes(), "John".getBytes());
        handler.add(account, "surname".getBytes(), "Doe".getBytes());
        handler.add(address, "street".getBytes(), "123 West Street".getBytes());
        handler.add(address, "zipcode".getBytes(), "2196".getBytes());
        handler.add(address, "suburb".getBytes(), "Sandton".getBytes());
        handler.add(address, "city".getBytes(), "Johannesburg".getBytes());
        handler.add(address, "state".getBytes(), "ZA".getBytes());
        handler.add(account, "region".getBytes(), "Gauteng".getBytes());

        Put handler2 = new Put("student2".getBytes());
        handler2.add(account, "name".getBytes(), "Bob".getBytes());
        handler2.add(account, "surname".getBytes(), "Martin".getBytes());
        handler2.add(address, "street".getBytes(), "01 Main Street".getBytes());
        handler2.add(address, "zipcode".getBytes(), "2122".getBytes());
        handler2.add(address, "suburb".getBytes(), "Sandton".getBytes());
        handler2.add(address, "city".getBytes(), "Johannesburg".getBytes());
        handler2.add(address, "state".getBytes(), "ZA".getBytes());
        handler2.add(account, "region".getBytes(), "Gauteng".getBytes());

        table.put(handler);
        table.put(handler2);
        table.close();
    }
}
