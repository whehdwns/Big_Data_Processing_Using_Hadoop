
package bdpuh.assignment4;


import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
/**
 *
 * @author hdadmin
 */


public class RatingDistribution {
        public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
        
        String indir = args[0]; // "/movie-ratings"; //args[0];
        String outdir = args[1]; //"/movie-rating-distribution";//args[1];
        
        Job ratingdistJob = null;
        
        Configuration conf = new Configuration();
        
        conf.setInt("mapreduce.job.reduces",0);
//        conf.addResource(new Path("/usr/local/hadoop/hadoop-3.3.4/etc/hadoop/core-site.xml"));
//        conf.addResource(new Path("/usr/local/hadoop/hadoop-3.3.4/etc/hadoop/hdfs-site.xml"));
//        conf.addResource(new Path("/usr/local/hadoop/hadoop-3.3.4/etc/hadoop/mapred-site.xml"));   
 
        try {
            //ratingdistJob = new Job(conf, "RatingsCount");
            ratingdistJob = Job.getInstance(conf, "RatingDistribution");
        
        } catch (IOException ex) {
            Logger.getLogger(RatingDistribution.class.getName()).log(Level.SEVERE, null, ex);
            return;
        }
        
        //Specify the input path
        try {
            FileInputFormat.addInputPath(ratingdistJob, new Path(indir));
        } catch (IOException ex) {
            Logger.getLogger(RatingDistribution.class.getName()).log(Level.SEVERE, null, ex);
            return;
        }
        
        //Set the Input Data Format
        ratingdistJob.setInputFormatClass(TextInputFormat.class);
        
        //Set the Mapper and Reducer class
        ratingdistJob.setMapperClass(RatingDistributionMapper.class);
        ratingdistJob.setReducerClass(RatingDistributionReducer.class);
        
        //Set the Jar File
        ratingdistJob.setJarByClass(bdpuh.assignment4.RatingDistribution.class);
        
        //Set the output path
        FileOutputFormat.setOutputPath(ratingdistJob, new Path(outdir));
        
        //Set the output data format
        ratingdistJob.setOutputFormatClass(TextOutputFormat.class);
        
        //Set the Output Key and Value Class
        ratingdistJob.setOutputKeyClass(Text.class);
        ratingdistJob.setOutputValueClass(IntWritable.class);

        try {
            ratingdistJob.waitForCompletion(true);
        } catch (IOException ex) {
            Logger.getLogger(RatingDistribution.class.getName()).log(Level.SEVERE, null, ex);
            return;
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(RatingDistribution.class.getName()).log(Level.SEVERE, null, ex);
        }    
    }
}

