/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package bdpuh.assignment6;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;
/**
 *
 * @author hdadmin
 */
public class MovieRatingsMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
    Logger logger = Logger.getLogger(MovieRatingsMapper.class);
    IntWritable moviekey = new IntWritable();
    String fileName = "";
    Text field =  new Text();
    StringBuilder stringBuilder = new StringBuilder();
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        logger.debug("DEBUG – IN MAPPER SETUP");    
        super.setup(context);
        logger.info("in setup of " + context.getTaskAttemptID().toString());
        fileName = ((FileSplit) context.getInputSplit()).getPath() + "";
    }
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        logger.debug("DEBUG - PROCESS A RECORD");
        String line = value.toString();
        //data.gz file. data file
        if (fileName.endsWith(".data.gz")) {
            String[] split = line.split("\t");
            moviekey.set(Integer.parseInt(split[1]));
            stringBuilder.append("D|").append(split[0]).append("|").append(split[2]);
        }else{
            //item.gz file. item file
            String[] split = line.split("\\|");
            moviekey.set(Integer.parseInt(split[0]));
            stringBuilder.append("I|").append(split[1]).append("|").append(split[2]).append("|").append(split[4]);
        }
        field.set(stringBuilder.toString());
        stringBuilder.delete(0, stringBuilder.length());
        context.write(moviekey, field);
        // count total records
        Counter counter = context.getCounter(MovieRatingsCounter.TOTAL_RECORDS);
        counter.increment(1);
    }
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        logger.debug("DEBUG – IN MAPPER CLEANUP");
    }
}
