package edu.supmti.hadoop;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class HadoopFileStatus {
    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.err.println("Usage: <hdfs_dir> <filename> [newfilename]");
            System.err.println("Ex: /user/root/input purchases.txt achats.txt");
            System.exit(1);
        }

        String dir = args[0];
        String filename = args[1];
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        Path filepath = new Path(dir, filename);
        if (!fs.exists(filepath)) {
            System.out.println("File does not exist: " + filepath);
            fs.close();
            System.exit(1);
        }

        FileStatus status = fs.getFileStatus(filepath);
        System.out.println("File Name: " + filepath.getName());
        System.out.println("File Size: " + status.getLen() + " bytes");
        System.out.println("File owner: " + status.getOwner());
        System.out.println("File permission: " + status.getPermission());
        System.out.println("File Replication: " + status.getReplication());
        System.out.println("File Block Size: " + status.getBlockSize());

        BlockLocation[] blockLocations = fs.getFileBlockLocations(status, 0, status.getLen());
        for (BlockLocation blockLocation : blockLocations) {
            String[] hosts = blockLocation.getHosts();
            System.out.println("Block offset: " + blockLocation.getOffset());
            System.out.println("Block length: " + blockLocation.getLength());
            System.out.print("Block hosts: ");
            for (String host : hosts) {
                System.out.print(host + " ");
            }
            System.out.println();
        }

        // optional: rename if new name provided
        if (args.length >= 3) {
            Path newpath = new Path(dir, args[2]);
            boolean ok = fs.rename(filepath, newpath);
            System.out.println(ok ? "Renamed to " + newpath : "Rename failed");
        }

        fs.close();
    }
}
