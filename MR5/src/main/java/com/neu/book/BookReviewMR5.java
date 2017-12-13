/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.neu.book;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class BookReviewMR5 {
    public static void main(String[] args)throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.addResource(new Path("C:/hadoop/hadoop-2.8.1/etc/hadoop/core-site.xml"));
	    conf.addResource(new Path("C:/hadoop/hadoop-2.8.1/etc/hadoop/hdfs-site.xml"));
		Job job = Job.getInstance(conf,"BookReviewsMR5");
		job.setJarByClass(BookReviewMR5.class);
		job.setMapperClass(Mapper1.class);
		job.setReducerClass(Reducer1.class);
		//Setting output key and value class types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
				
		//Setting up input and output directories
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
				
		//Run the job and wait till its completed
		System.exit(job.waitForCompletion(true) ? 0 :1);
    }   
}
