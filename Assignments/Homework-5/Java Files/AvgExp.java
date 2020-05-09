/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
//import java.util.StringTokenizer;
import java.lang.String;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.util.HashMap;
import java.util.Map;
import java.lang.Object;


public class AvgExp {

  private static String MAX_COUNT = "MAX_COUNT";
  private static String AVG_LEXP = "AVG_LEXP";
  private static Map<String, Float> calcAvg(Iterable<FloatWritable> lifeExpect) {
    Map<String, Float> resMap = new HashMap<>();
    float avg_le = 0;
    float count = 0;

    for (FloatWritable lExp : lifeExpect){
      count++;
      avg_le = avg_le + lExp.get();
    } 
    avg_le = avg_le/count;

    resMap.put(MAX_COUNT, count);
    resMap.put(AVG_LEXP, avg_le);
    return resMap;
  }
  


  public static class SplitMapper extends Mapper<Object, Text, Text, FloatWritable>{

    public void map(Object offset, Text rows, Context context) throws IOException, InterruptedException {
      
      String[] cols = rows.toString().split(",");
      if (Float.parseFloat(cols[8].replaceAll("'","")) > 10000){
        context.write(new Text(cols[2]), new FloatWritable(Float.parseFloat(cols[7].replaceAll("'",""))));
      }
    }
  }

  public static class CountReducer extends Reducer<Text, FloatWritable, Text, FloatWritable>{

    public void reduce(Text continent, Iterable<FloatWritable> lifeExpectancy, Context context) throws IOException, InterruptedException {
      Map<String, Float> countryInfo = calcAvg(lifeExpectancy);
      if (countryInfo.get(MAX_COUNT) >= 5){
        context.write(continent, new FloatWritable(countryInfo.get(AVG_LEXP)));
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      System.err.println("Usage: wordcount <in> [<in>...] <out>");
      System.exit(2);
    }
    Job job = Job.getInstance(conf, "avg exp");
    job.setJarByClass(AvgExp.class);
    job.setMapperClass(SplitMapper.class);
    //job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(CountReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FloatWritable.class);
    for (int i = 0; i < otherArgs.length - 1; ++i) {
      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
    }
    FileOutputFormat.setOutputPath(job,
      new Path(otherArgs[otherArgs.length - 1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
