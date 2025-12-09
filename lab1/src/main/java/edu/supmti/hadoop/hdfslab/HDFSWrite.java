package edu.supmti.hadoop;

import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class HDFSWrite {
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.err.println("Usage: <hdfs_path> [text to write]");
            System.err.println("Ex: /user/root/input/bonjour.txt \"Salut tout le monde\"");
            System.exit(1);
        }

        Path path = new Path(args[0]);
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        if (fs.exists(path)) {
            System.err.println("File already exists: " + path);
            fs.close();
            System.exit(1);
        }

        String content = (args.length > 1) ? String.join(" ", Arrays.copyOfRange(args, 1, args.length))
                                           : "Bonjour tout le monde !";

        try (FSDataOutputStream out = fs.create(path)) {
            out.write(content.getBytes("UTF-8"));
        }

        System.out.println("Written to " + path);
        fs.close();
    }
}
