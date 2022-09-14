
package bdpuh.hw2;

import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class Copy_PThread implements Runnable {
    private Configuration conf;
    private Path file;
    private String hdfsdir;
    
    public Copy_PThread(Configuration conf, Path file, String hdfsdir){
        this.conf = conf;
        this.file = file;
        this.hdfsdir = hdfsdir;
    }
    @Override
    public void run() {
       System.out.println("Start thread: " + Thread.currentThread().getName());
       processCommand();
       System.out.println("End thread: "+ Thread.currentThread().getName());
       return;
    }
    public void processCommand(){
        Copying(conf, file,hdfsdir);
    }
    private static void Copying(Configuration conf, Path file, String hdfsdir){
       Path hdfs_source = new Path(hdfsdir); 
       FileSystem fileSystem = null;
        try {
            fileSystem = FileSystem.get(conf);
        } catch (IOException ex) {
            System.err.println("Unable to get Configuration");
        }    
        try {
            System.out.println("Copying "+ file.getName());
            fileSystem.copyFromLocalFile(file, hdfs_source);
        } catch (IOException ex) {
             System.err.println("Unable to Copy files");
        }     
    }
}
