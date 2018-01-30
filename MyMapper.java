Part 1 - Find most active users

*/
package part1;
import java.io.IOException;
import java.sql.Timestamp;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;

import java.text.ParseException;
import java.text.SimpleDateFormat;

public class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private final static IntWritable ONE = new IntWritable(1);
    SimpleDateFormat simpleDateFormat1 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");
    
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
     // Retrieve command arguments from mainSort class		
		Configuration config = context.getConfiguration();
		Timestamp ts_start = new Timestamp(config.getLong("timestamp0", 99999999));
	    Timestamp ts_end = new Timestamp(config.getLong("timestamp1", 120000000));
	   
	 // Read the line
	    String line = ((Text) value).toString();
	 // Break down the line into strings(split each word by space) 
		String[] temp = line.split(" ");
  	
		// Only want the first line of each revision record
        // Check if the length of the line is at least 9, which is greater than the word'REVISION' plus a whitespace
		// By doing this we decide whether it is worthwhile to check the line further
		if(line.length() > 9) {
		   // Check if the first word is "REVISION" 
			if(temp[0].equals("REVISION")){
			
			  // If the first word is REVISION, do following		    
			    long revision = 0;
		  	    try {
		  	        // Retrieve this line's Timestamp
		  	    	    revision = simpleDateFormat1.parse(temp[4]).getTime();
				        Timestamp ts_revision = new Timestamp(revision);
				    
				    // Check if the Timestamp within the input time frame
				    // and if the user_id is numeric because we want to exclude user_id with ip addresses
					    if(ts_revision.after(ts_start) && ts_revision.before(ts_end) && isInteger(temp[6])){
				   
					// If the time is correct, write(user, 1) 
					// pass the occurrence of this user_id to Combiner
						      context.write(new Text(temp[6]), ONE);
						
					    }  
		  	    } catch (ParseException e) 
		  	                { e.printStackTrace(); }
		    }	
    	 }
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


}

