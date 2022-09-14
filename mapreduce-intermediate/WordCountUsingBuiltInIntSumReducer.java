package bdpuh.mapreduceintermediate;

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
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;

public class WordCountUsingBuiltInIntSumReducer {
    public static void main(String args[])  {
        Job wordCountJob = null;
        
        Configuration conf = new Configuration();        
        
        try {
            wordCountJob = new Job(conf, "WordCountUsingBuiltInIntReducer");
        } catch (IOException ex) {
            Logger.getLogger(WordCountUsingBuiltInIntSumReducer.class.getName()).log(Level.SEVERE, null, ex);
            return;
        }
        
        // Specify the Input path
        try {
            FileInputFormat.addInputPath(wordCountJob, new Path(args[0]));
        } catch (IOException ex) {
            Logger.getLogger(WordCountUsingBuiltInIntSumReducer.class.getName()).log(Level.SEVERE, null, ex);
            return;
        }

        // Set the Input Data Format
        wordCountJob.setInputFormatClass(TextInputFormat.class);
        
        // Set the Mapper and Reducer Class
        wordCountJob.setMapperClass(WordCountMapper.class);
        wordCountJob.setReducerClass(IntSumReducer.class);

        // Set the Jar file 
        wordCountJob.setJarByClass(WordCountUsingBuiltInIntSumReducer.class);       
        
        // Set the Output path
        FileOutputFormat.setOutputPath(wordCountJob, new Path(args[1]));
        
        // Set the Output Data Format
        wordCountJob.setOutputFormatClass(TextOutputFormat.class);        
        
        // Set the Output Key and Value Class
        wordCountJob.setOutputKeyClass(Text.class);
        wordCountJob.setOutputValueClass(IntWritable.class); 
        
              
             
        try {
            wordCountJob.waitForCompletion(true);
        } catch (IOException ex) {
            Logger.getLogger(WordCountUsingBuiltInIntSumReducer.class.getName()).log(Level.SEVERE, null, ex);
        } catch (InterruptedException ex) {
            Logger.getLogger(WordCountUsingBuiltInIntSumReducer.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(WordCountUsingBuiltInIntSumReducer.class.getName()).log(Level.SEVERE, null, ex);
        }
        

                
    }
    
}
