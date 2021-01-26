/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package profitparcountry;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author hadoop
 */
public class ProfitParCountry {

  public static void main(String[] args) throws IllegalArgumentException, IOException, InterruptedException, ClassNotFoundException {
      {
            // this main function will call run method defined above.
         Configuration conf = new Configuration();
          Job job = new Job(conf, "ProfitParCountry");
   

            //Setting configuration object with the Data Type of output Key and Value
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            //Providing the mapper and reducer class names
            job.setMapperClass(ModeleMapReduceMapper.class);
            job.setReducerClass(ModeleMapReduceReducer.class);
            //We wil give 2 arguments at the run time, one in input path and other is output path
            Path inp = new Path(args[0]);
            Path out = new Path(args[1]);
            //the hdfs input and output directory to be fetched from the command line
            FileInputFormat.addInputPath(job, inp);
            FileOutputFormat.setOutputPath(job, out);

          job.waitForCompletion(true);	
        
      }}}
