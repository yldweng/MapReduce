package part1;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;



public class mainSort extends Configured implements Tool {
	 static String filePath= " ";
	
	@Override
	public int run(String[] args) throws Exception {
		//Create a new configuration for storing the arguments
		Configuration config = new Configuration(getConf());
		
		// Retrieve arguments from command line
		int N = Integer.parseInt(args[0]);
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");
		long ts0 = simpleDateFormat.parse(args[1]).getTime();
		long ts1 = simpleDateFormat.parse(args[2]).getTime();

		//Store the arguments to the configuration, so each mapper and reducer can use it
		config.setInt("number_N", N);
		config.setLong("timestamp0", ts0);
		config.setLong("timestamp1", ts1);
		
		// create a job to run the code
		@SuppressWarnings("deprecation")
		Job job = new Job(config);
		job.setJobName("Find the most active user");
		job.setJarByClass(mainSort.class);
		
		// declare the Mapper, the reducer, the combiner and partitioner to be used.
		job.setMapperClass(MyMapper.class);
		job.setCombinerClass(MyCombiner.class);
		job.setReducerClass(MyReducer.class);
		
		//job.setPartitionerClass(MyPartitioner.class);
		// Input      *** TextInputFormat extends FileInputFormat<LongWritable, Text> ***
		job.setInputFormatClass(TextInputFormat.class);
	
		// Output
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setOutputFormatClass(TextOutputFormat.class);		
		FileInputFormat.addInputPath(job, new Path(args[3]));
		FileOutputFormat.setOutputPath(job, new Path(args[4]));
		
		if(job.waitForCompletion(true)){
		 File rootFile = new File(args[4]);
		 File[] listFile = rootFile.listFiles();
		 // If there is more than 2 files generated (in which there are more than 1 reducers)
		 // Then need to merge all output files
		 if(listFile.length > 2){
		    String newFile = args[4]+"/Merge.txt";
		    merge(listFile, newFile, N);
		 }
		}
		
		return job.waitForCompletion(true)?0:1;
	}

	public static void main(String[] args) throws Exception {
       System.exit(ToolRunner.run(new mainSort(), args));  
       
	}
	
	public static void merge(File[] listOfFiles, String newFile, int input_N) throws Exception {
		HashMap<String, IntWritable> totalCount_Map = new HashMap<String, IntWritable>();
		File fileInput = null;
			// build file output stream
		File fileOut = new File(newFile);
		FileOutputStream out = new FileOutputStream(fileOut);
			
		for (int i = 0; i < listOfFiles.length; i++) {
			// open file input stream
			fileInput = listOfFiles[i];
			FileInputStream in = new FileInputStream(fileInput);
			// Input data from input stream and write to output stream
			DataInputStream dataInput = new DataInputStream(in);
		    BufferedReader br = new BufferedReader(new InputStreamReader(dataInput));
		    String Line = null;
		    
		    // Iterate the file
		    while ((Line = br.readLine()) != null)   {
		        // Break the line into tokens	
		    	StringTokenizer st = new StringTokenizer(Line);
                // If there is more than one token
		    	if(st.countTokens()>1){
		    		String key1 = st.nextToken();
		    	// Check if the first token is Integer (article_id / user_id)	
		    		if(isInteger(key1)){
		    	// Put key-value pair into the map		
		    	  totalCount_Map.put(key1, new IntWritable(Integer.parseInt(st.nextToken())));
		    		}	
		    	}
		    }
		    // close the input stream
		    in.close();
		}
		Map<String, IntWritable> sortedMap = sortMap(totalCount_Map);
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));
		int topN = 0;
		for (String key: sortedMap.keySet()) {
            // If already output the Nth user, then stop
        	if (topN ++ == input_N) {
                break;
            }
            bw.write(key + " " + sortedMap.get(key));
            bw.newLine();
        }   
		bw.close();
		out.close();
	 }
	
	
	// Method for checking if a string is an integer
		public static boolean isInteger(String s) {
		    try { 
		        Integer.parseInt(s); 
		    } catch(NumberFormatException e) { 
		        return false; 
		    } catch(NullPointerException e) {
		        return false;
		    }
		    return true;
		}

	// Method for sorting values in the Map, since hadoop automatically sort on Key, but not on values.
	
	@SuppressWarnings({ "rawtypes", "hiding" })
	public static <String extends Comparable,IntWritable extends Comparable> Map<String,IntWritable> sortMap(Map<String,IntWritable> Map){
      
	    // A list contains a Set view of the key-value pairs contained in the map
		// The set is backed by the map, so changes to the map are reflected in the set, and vice-versa.
		List<Map.Entry<String,IntWritable>> kvSet = new LinkedList<Map.Entry<String,IntWritable>>(Map.entrySet());
     
        //Sorts the list according to the order induced by the specified comparator
        Collections.sort(kvSet, new Comparator<Map.Entry<String,IntWritable>>() {

            @SuppressWarnings("unchecked")
			@Override
            public int compare(Entry<String,IntWritable> kv1, Entry<String,IntWritable> kv2) {
                // Compare values first
            	int compare = kv2.getValue().compareTo(kv1.getValue());
            	// If two values are not equal, sort the values in descending order (return the integer -1 or 1)
                if (compare != 0) {
                     return compare;
                } 
                // If two values are equal, sort the key in ascending order
                else {
                	String skv1 = kv1.getKey();
               	    String skv2 = kv2.getKey();
               	    Integer kv11 = Integer.parseInt((java.lang.String) skv1);
               	    Integer kv22 = Integer.parseInt((java.lang.String) skv2);
                    return kv11.compareTo(kv22);
                }
            }
        }
        );
        // The linked list defines the iteration ordering, which is normally the order in which keys were inserted into the map (insertion-order)
        // This is useful since the keys is in natural ordering 
        Map<String,IntWritable> sortedMap = new LinkedHashMap<String,IntWritable>();
        
        // Put the list of key-value pairs into the Map
        for(Map.Entry<String,IntWritable> kvPair: kvSet){
            sortedMap.put(kvPair.getKey(), kvPair.getValue());
        }
        // Return the map with sorted set of key-value pairs
        return sortedMap;
    }
	
}
