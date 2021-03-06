import java.io.IOException;
import java.util.StringTokenizer;

// REGEX
import java.util.regex.*;

// Library to convert timestamp to Java LocalDate
import java.time.LocalDate;

// Library to parse JSON file
import org.json.JSONObject;

// Hadoop libraries
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
                        // Only time part of timestamp required to get the LocalDate
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

                        // Check if recognized
                        boolean isRecognized = jo.getBoolean("recognized");

                        // 2 different keys based on 2 different conditions
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


        // Change data type of output key of reducer in function header to Null, since you don't want to print key (only value)
        public static class InstanceReducer extends Reducer<Text, IntWritable, NullWritable, IntWritable>{

                private IntWritable result = new IntWritable();

                public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{

                        // Get total count
                        int sum = 0;
                        for(IntWritable val:values){
                                sum += val.get();
                        }

                        result.set(sum);

                        // We want only the value, hence write Null for key.
                        context.write(NullWritable.get(), result);
                }

        }


        public static void main(String[] args) throws Exception{

                Configuration conf = new Configuration();

                // Given word is passed in command line (the word whose count is to be checked based on the conditions)
                conf.set("givenWord", args[2]);

                // Map Reduce Configurations
                Job job = Job.getInstance(conf, "my instance count");
                job.setJarByClass(ObjectInstancesByRecognized.class);
                job.setMapperClass(InstanceMapper.class);
                job.setReducerClass(InstanceReducer.class);
                // Let data type of mapper output key be Text
                job.setMapOutputKeyClass(Text.class);

                // Reflect change in data type of output key expected from reducer to Null, since we don't wan't to print anything for key
                job.setOutputKeyClass(NullWritable.class);
                job.setOutputValueClass(IntWritable.class);

                FileInputFormat.addInputPath(job, new Path(args[0]));
                FileOutputFormat.setOutputPath(job, new Path(args[1]));
                System.exit(job.waitForCompletion(true) ? 0 : 1);

        }

}
