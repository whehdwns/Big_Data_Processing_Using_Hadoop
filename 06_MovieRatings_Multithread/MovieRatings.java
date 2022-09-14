package bdpuh.assignment6;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
/**
 *
 * @author hdadmin
 */
public class MovieRatings {
    public static void main(String args[])  {
        
        String indir = args[0]; 
        String outdir = args[1]; 
        
//        String indir = "/movie-and-ratings"; //args[0];
//        String outdir = "/movie-rating-result";
        
        Job movieratingJob = null;    
        Configuration conf = new Configuration();
        
        conf.set("mapreduce.framework.name", "local");
        
        conf.set("mapreduce.map.log.level", "DEBUG");
        conf.set("mapreduce.reduce.log.level", "DEBUG");

        conf.setBoolean("mapred.output.compress", true);
        conf.setStrings("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        
        //Intermediate Mapper/Reducer Compression
        conf.setBoolean("mapred.compress.map.output", true);
        conf.setStrings("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
        
        try {
            movieratingJob = new Job(conf, "MovieRatings");
        } catch (IOException ex) {
            Logger.getLogger(MovieRatings.class.getName()).log(Level.SEVERE, null, ex);
            return;
        }
        
        // Specify the Input path
        try {
            FileInputFormat.addInputPath(movieratingJob, new Path(indir));
        } catch (IOException ex) {
            Logger.getLogger(MovieRatings.class.getName()).log(Level.SEVERE, null, ex);
            return;
        }


        
        // Set the Input Data Format
        movieratingJob.setInputFormatClass(TextInputFormat.class);
        
        // Set the Mapper and Reducer Class
        movieratingJob.setMapperClass(MovieRatingsMapper.class);
        movieratingJob.setReducerClass(MovieRatingsReducer.class);
        //movieratingJob.setReducerClass(MovieRatingsCombiner.class);

        // Set the Jar file 
        movieratingJob.setJarByClass(bdpuh.assignment6.MovieRatings.class);       
        
        // Set the Output path
        FileOutputFormat.setOutputPath(movieratingJob, new Path(outdir));
        
        // Set the Output Data Format
        movieratingJob.setOutputFormatClass(TextOutputFormat.class);        
        
        // Set the Output Key and Value Class
        movieratingJob.setOutputKeyClass(IntWritable.class);
        movieratingJob.setOutputValueClass(Text.class); 
        
        //movieratingJob.setNumReduceTasks(2);
             
        try {
            movieratingJob.waitForCompletion(true);
        } catch (IOException ex) {
            Logger.getLogger(MovieRatings.class.getName()).log(Level.SEVERE, null, ex);
        } catch (InterruptedException ex) {
            Logger.getLogger(MovieRatings.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(MovieRatings.class.getName()).log(Level.SEVERE, null, ex);
        }
        

                
    }
    
}
