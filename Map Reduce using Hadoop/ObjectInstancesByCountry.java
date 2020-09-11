import java.io.IOException;
import java.util.StringTokenizer;

// Library to convert timestamp to Java LocalDate
import java.time.LocalDate;

// Libraries to parse and perform operations on JSON
import org.json.JSONObject;
import org.json.JSONArray;

// Hadoop Libraries
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


public class ObjectInstancesByCountry {

        public static class InstanceMapper extends Mapper<Object, Text, Text, IntWritable>{

                private final static IntWritable one = new IntWritable(1);
                private Text word = new Text();

                public boolean distanceIsLonger(int firstXCood, int firstYCood, int passedDistance){


                        // Calculate Euclidean distance of point in stroke from origin
                        double xdist = Math.abs((double)firstXCood-0);
                        double ydist = Math.abs((double)firstYCood-0);
                        double distanceFromOrigin = Math.sqrt((ydist*ydist)+(xdist*xdist));

                        // Check if the Euclidean distance calculated above is greater than passed distance
                        if(distanceFromOrigin > passedDistance){
                                return true;
                        }

                        return false;

                }

                public void map(Object key, Text value, Context context) throws IOException, InterruptedException{

                        Configuration conf = context.getConfiguration();

                        JSONObject jo = new JSONObject(value.toString());

                        // Get the strokes of the current record
                        JSONArray currentStrokes = (JSONArray)jo.get("drawing");
                        JSONArray firstStroke = (JSONArray)currentStrokes.get(0);
                        JSONArray xcood = (JSONArray)firstStroke.get(0);
                        int firstXCood = (int)xcood.get(0);
                        JSONArray ycood = (JSONArray)firstStroke.get(1);
                        int firstYCood = (int)ycood.get(0);


                        // Get the distance passed in command line argument
                        String passedDist = conf.get("givenDistance");
                        int passedDistance = Integer.parseInt(passedDist);

                        // Check if the distance of stroke is greater than the distance passed in command line
                        boolean distanceIsGreater = distanceIsLonger(firstXCood, firstYCood, passedDistance);



                        // Get current word
                        String currentWord = jo.getString("word");

                        // Get word passed in command line argument
                        String passedWord = conf.get("givenWord");

                        // Get countrycode
                        String countrycode = jo.getString("countrycode");
                        word.set(countrycode);

                        if(passedWord.equals(currentWord) && distanceIsGreater)
                                context.write(word, one);


                }

        }

        public static class InstanceReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

                private IntWritable result = new IntWritable();

                public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{

                        // Count
                        int sum = 0;
                        for(IntWritable val:values){
                                sum += val.get();
                        }

                        result.set(sum);
                        context.write(key, result);
                }


        }


        public static void main(String[] args) throws Exception{

                Configuration conf = new Configuration();

                // After input and output path:
                // First word passed in command line is the word whose count is to be found based on the conditions
                // Second word is the minimum distance
                conf.set("givenWord", args[2]);
                conf.set("givenDistance", args[3]);

                // Map Reduce configirations
                Job job = Job.getInstance(conf, "my country count");
                job.setJarByClass(ObjectInstancesByCountry.class);
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
