import java.io.IOException;
import java.util.StringTokenizer;

import org.json.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class CountInstances {

        public static class InstanceMapper extends Mapper<Object, Text, Text, IntWritable>{

                private final static IntWritable one = new IntWritable(1);
                private Text word = new Text();

                public void map(Object key, Text value, Context context) throws IOException, InterruptedException{

                        // StringTokenizer itr = new StringTokenizer(value.toString());

                        // while (itr.hasMoreTokens()){

                        //      word.set(itr.nextToken())

                        JSONObject jo = new JSONObject(value.toString());

                        // Get current word                     
                        //String search =  new String("word");
                        String currentWord = jo.getString("word");


                        // Get word passed in command line argument
                        Configuration conf = context.getConfiguration();

                        String passedWord = conf.get("givenWord");

                        // find if reserved
                        boolean isReserved = jo.getBoolean("recognized");
                        if(currentWord.equals(passedWord) && isReserved){
                                Text thewordis = new Text(currentWord);
                                context.write(thewordis, one);
                        }

                        //}

                }

        }

        public static class InstanceReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

                private IntWritable result = new IntWritable();

                public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{

                        int sum = 0;
                        for(IntWritable val:values){
                                sum += val.get();
                        }

                        result.set(sum);
                        Text dummy = new Text("");
                        context.write(dummy, result);
                }

        }


        public static void main(String[] args) throws Exception{

                Configuration conf = new Configuration();
                conf.set("givenWord", args[2]);
                //addJarToDistributedCache(JSONObject.class,conf);
                Job job = Job.getInstance(conf, "my instance count");
                //job.addFileToClassPath(new Path("json-java.jar"));
                job.setJarByClass(CountInstances.class);
                job.setMapperClass(InstanceMapper.class);
                job.setCombinerClass(InstanceReducer.class);
                job.setReducerClass(InstanceReducer.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(IntWritable.class);
                FileInputFormat.addInputPath(job, new Path(args[0]));
                FileOutputFormat.setOutputPath(job, new Path(args[1]));
                System.exit(job.waitForCompletion(true) ? 0 : 1);

        }

}


// NOT currentWord == passedWord - this gave no ouput! Have to use String ".equals()"
