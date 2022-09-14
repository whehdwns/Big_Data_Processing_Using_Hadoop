package bdpuh.mapreduceintermediate;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.fieldsel.FieldSelectionMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class FieldSelectionMapperDemo {
    public static void main(String args[])  {
        Job fieldSelectionJob = null;
        
        Configuration conf = new Configuration();   
        conf.setStrings("mapreduce.fieldsel.data.field.separator", ",");
        conf.setStrings("mapreduce.fieldsel.map.output.key.value.fields.spec", "0,2,4:1,3,4-7,8-");
        
        
        try {
            fieldSelectionJob = new Job(conf, "FieldSelectionDemo");
        } catch (IOException ex) {
            Logger.getLogger(FieldSelectionMapperDemo.class.getName()).log(Level.SEVERE, null, ex);
            return;
        }
        
        // Specify the Input path
        try {
            FileInputFormat.addInputPath(fieldSelectionJob, new Path(args[0]));
        } catch (IOException ex) {
            Logger.getLogger(FieldSelectionMapperDemo.class.getName()).log(Level.SEVERE, null, ex);
            return;
        }

        // Set the Input Data Format
        fieldSelectionJob.setInputFormatClass(TextInputFormat.class);
        
        // Set the Mapper and Reducer Class
        fieldSelectionJob.setMapperClass(FieldSelectionMapper.class);
        fieldSelectionJob.setReducerClass(Reducer.class);

        // Set the Jar file 
        fieldSelectionJob.setJarByClass(FieldSelectionMapperDemo.class);       
        
        // Set the Output path
        FileOutputFormat.setOutputPath(fieldSelectionJob, new Path(args[1]));
        
        // Set the Output Data Format
        fieldSelectionJob.setOutputFormatClass(TextOutputFormat.class);        
        
        // Set the Output Key and Value Class
        fieldSelectionJob.setOutputKeyClass(Text.class);
        fieldSelectionJob.setOutputValueClass(Text.class); 
              
             
        try {
            fieldSelectionJob.waitForCompletion(true);
        } catch (IOException ex) {
            Logger.getLogger(FieldSelectionMapperDemo.class.getName()).log(Level.SEVERE, null, ex);
        } catch (InterruptedException ex) {
            Logger.getLogger(FieldSelectionMapperDemo.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(FieldSelectionMapperDemo.class.getName()).log(Level.SEVERE, null, ex);
        }
        

                
    }
    
}
