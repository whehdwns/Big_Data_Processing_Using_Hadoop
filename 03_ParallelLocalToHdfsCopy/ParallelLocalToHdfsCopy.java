package bdpuh.hw2;

import java.io.*;
import java.net.URI;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.GzipCodec;
/**
 *
 * @author hdadmin
 */
public class ParallelLocalToHdfsCopy {
    
    public static void main(String[] args) throws IOException{
        
        if (args.length < 3){
            System.out.println("Please enter local directory, source directory, and number of threads for copying");
            System.exit(0);
        }
        String local_filesystem = args[0];
        String hdfs_filesystem = args[1];
        int thread_cnt = Integer.parseInt(args[2]);

        Configuration conf = new Configuration();
        conf.addResource(new Path("/usr/local/hadoop/hadoop-3.3.4/etc/hadoop/core-site.xml"));
        conf.addResource(new Path("/usr/local/hadoop/hadoop-3.3.4/etc/hadoop/hdfs-site.xml"));      
        Path local_source = new Path(local_filesystem);
        Path hdfs_source = new Path(hdfs_filesystem); 

        
        FileSystem fileSystem = null;
        fileSystem = FileSystem.get(conf);
        
        LocalFileSystem local_file = FileSystem.getLocal(conf);
        File local_dir = new File(local_filesystem);
        
        File dest_dir = new File(hdfs_filesystem);      
        //Check if the local files system exist
        if (!local_dir.isDirectory()){
            System.out.println("Source directory " + local_dir + " does not exist.");
            System.exit(0);
        }
        if (dest_dir.isDirectory()){
            System.out.println("Destination directory already exists. Delete before running the program");
            System.exit(0);
        }else{
            System.out.println("Local File Path: " + local_filesystem + "\n" + 
                               "HDFS Path: " + hdfs_filesystem + "\n."  + 
                                "Thread Count: "+ thread_cnt + "\n." );
            boolean hdfs_dir = true;
            try{
                //Create Directory in HDFS
                hdfs_dir = fileSystem.mkdirs(hdfs_source);
                System.out.println ("Created dir successfully: " + hdfs_dir); 
                FileStatus[] file_local = null;
                file_local  = local_file.listStatus(local_source);
                
                // ExecutorService to copy file in parallel.
                ExecutorService copy_executor = Executors.newFixedThreadPool(thread_cnt);
                for (int i = 0; i < file_local.length; i++) {
                    
                    Runnable copy_worker = new Copy_PThread(conf, file_local[i].getPath(), hdfs_filesystem);
                    copy_executor.execute(copy_worker);
                }
                copy_executor.shutdown();
                while (!copy_executor.isTerminated()) { 
                
                }
                
                //Compression
                FSDataInputStream fSDataInputStream = null;
                FSDataOutputStream fsDataOutputStream = null;
                CompressionCodec compressionCodec = new GzipCodec();
                CompressionOutputStream compressedOutputStream = null;

                Path[] files_dir = null;
                files_dir = FileUtil.stat2Paths(fileSystem.listStatus(hdfs_source));
                for (Path path: files_dir){
                    fSDataInputStream = fileSystem.open(path);
                    Path compressedFileToWrite = new Path(path + ".gz");
                    fsDataOutputStream = fileSystem.create(compressedFileToWrite);
                    compressedOutputStream= compressionCodec.createOutputStream(fsDataOutputStream);
                    IOUtils.copyBytes(fSDataInputStream, compressedOutputStream, conf);
                }
                System.out.println("Compression is finished.");
                fSDataInputStream.close();
                fsDataOutputStream.close();
                compressedOutputStream.close();
                fileSystem.close();

            }catch (IOException ex){
                System.err.printf(ex.getMessage());
            }        
        }
    }

}


