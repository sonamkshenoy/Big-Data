import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.*;
import java.time.LocalDate;

import org.json.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class ObjectInstancesByRecognized {

        public static class InstanceMapper extends Mapper<Object, Text, Text, IntWritable>{

                private final static IntWritable one = new IntWritable(1);
                private Text word = new Text();

                // function to check if the given timestamp falls on a Saturday or Sunday
                public boolean timestamp_is_weekend(String wordTimestamp){
                        String timePart = wordTimestamp.substring(0, 10);
                        LocalDate dt = LocalDate.parse(timePart);
                        String dayOfWeek = dt.getDayOfWeek().name();

                        String saturday = "SATURDAY";
                        String sunday = "SUNDAY";

                        return (dayOfWeek.equals(saturday) || dayOfWeek.equals(sunday));
                }

                // function to check if a record is valid
                public boolean is_valid_record(JSONObject record){
                        // word should have alphabets and whitespace only.
                        // Use Regex
                        Pattern p1 = Pattern.compile("^[ A-Za-z]+$");
                        Matcher m1 = p1.matcher(record.getString("word"));
                        if(!m1.matches())
                              return false;

                        // countrycode should contain only 2 uppercase letters
                        // Use Regex
                        Pattern p2 = Pattern.compile("^[A-Z]{2}$");
                        Matcher m2 = p2.matcher(record.getString("countrycode"));
                        if(!m2.matches())
                              return false;

                        // recognized should be true or false
                        boolean recog = record.getBoolean("recognized");
                        if(!(recog==true) && !(recog==false))
                              return false;

                        // key_id should be numeric string containing 16 characters only
                        Pattern p3 = Pattern.compile("^[0-9]{16}$");
                        Matcher m3 = p3.matcher(record.getString("key_id"));
                        if(!m3.matches())
                              return false;

                        return true;
                }

                public void map(Object key, Text value, Context context) throws IOException, InterruptedException{

                        // Get current record (JSON)
                        JSONObject jo = new JSONObject(value.toString());
                        boolean is_valid = is_valid_record(jo);

                        // Get current word
                        String currentWord = jo.getString("word");

                        // Get word passed in command line argument
                        Configuration conf = context.getConfiguration();
                        String passedWord = conf.get("givenWord");

                        // Get timestamp
                        String currentWordTimestamp = jo.getString("timestamp");
                        boolean isWeekend = timestamp_is_weekend(currentWordTimestamp);

                        // find if reserved
                        boolean isRecognized = jo.getBoolean("recognized");

                        if(is_valid){
                          if(currentWord.equals(passedWord)){
                            if(isRecognized){
                              Text thekey1 = new Text("Recognized");
                              context.write(thekey1, one);
                            }
                            // Check if the non-recognized word falls on a Saturday or Sunday
                            else if(!isRecognized && isWeekend){
                              Text thekey2 = new Text("notRecognized");
                              context.write(thekey2, one);
                            }
                          }
                       }
                }

        }


        public static class InstanceReducer extends Reducer<Text, IntWritable, NullWritable, IntWritable>{

                private IntWritable result = new IntWritable();

                public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{

                        int sum = 0;
                        for(IntWritable val:values){
                                sum += val.get();
                        }

                        result.set(sum);

                        // We want only the value
                        context.write(NullWritable.get(), result);
                }

        }


        public static void main(String[] args) throws Exception{

                Configuration conf = new Configuration();
                conf.set("givenWord", args[2]);
                Job job = Job.getInstance(conf, "my instance count");
                job.setJarByClass(ObjectInstancesByRecognized.class);
                job.setMapperClass(InstanceMapper.class);
                job.setReducerClass(InstanceReducer.class);
                job.setMapOutputKeyClass(Text.class);
                job.setOutputKeyClass(NullWritable.class);
                job.setOutputValueClass(IntWritable.class);
                FileInputFormat.addInputPath(job, new Path(args[0]));
                FileOutputFormat.setOutputPath(job, new Path(args[1]));
                System.exit(job.waitForCompletion(true) ? 0 : 1);

        }

}
