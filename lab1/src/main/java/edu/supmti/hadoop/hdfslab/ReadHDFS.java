package edu.supmti.hadoop;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class ReadHDFS {
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.err.println("Usage: <hdfs_path>");
            System.err.println("Ex: /user/root/purchases.txt");
            System.exit(1);
        }

        Path path = new Path(args[0]);
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        if (!fs.exists(path)) {
            System.err.println("File not found: " + path);
            fs.close();
            System.exit(1);
        }

        try (FSDataInputStream inStream = fs.open(path);
             InputStreamReader isr = new InputStreamReader(inStream, "UTF-8");
             BufferedReader br = new BufferedReader(isr)) {

            String line;
            while ((line = br.readLine()) != null) {
                System.out.println(line);
            }
        }

        fs.close();
    }
}
