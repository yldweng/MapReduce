/*
Part 1 - Find most active users
*/
package part1;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
  
	// A Hashmap for storing total counts of each user (in user_id)
	private HashMap<Text, IntWritable> totalCount_Map = new HashMap<Text, IntWritable>();
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {

		// the reduce method is ran once per key, not key-value pair. 
		// Each value with that key is presented in the Iterator
		Iterator<IntWritable> iter = values.iterator();
		int sum = 0;
		
		// add all value of key-value pairs
		while (iter.hasNext()) {
			sum += iter.next().get();
		}
       
		totalCount_Map.put(new Text(key), new IntWritable(sum));
	
	}
	
	
	@Override
	protected void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
    	// Retrieve input N from command line	
		Configuration config = context.getConfiguration();
		int input_N = Integer.parseInt(config.get("number_N"));
	
	    // Sort the hashmap that stores each user_id and its corresponding occurrences	
		Map<Text, IntWritable> sortedMap = sortMap(totalCount_Map);
	
	    // counter	
		int topN = 0;
		
		// Iterate the Map and output Top N users
        for (Text key: sortedMap.keySet()) {
            // If already output the Nth user, then stop
        	if (topN ++ == input_N) {
                break;
            }
            context.write(key, sortedMap.get(key));
        }    
	}

  // Method for sorting values in the Map, since hadoop automatically sort on Key, but not on values.
	
	@SuppressWarnings({ "rawtypes", "hiding" })
	public static <Text extends Comparable,IntWritable extends Comparable> Map<Text,IntWritable> sortMap(Map<Text,IntWritable> Map){
      
	    // A list contains a Set view of the key-value pairs contained in the map
		// The set is backed by the map, so changes to the map are reflected in the set, and vice-versa.
		List<Map.Entry<Text,IntWritable>> kvSet = new LinkedList<Map.Entry<Text,IntWritable>>(Map.entrySet());
     
        //Sorts the list according to the order induced by the specified comparator
        Collections.sort(kvSet, new Comparator<Map.Entry<Text,IntWritable>>() {

            @SuppressWarnings("unchecked")
			@Override
            public int compare(Entry<Text,IntWritable> kv1, Entry<Text,IntWritable> kv2) {
                // Compare values first
            	int compare = kv2.getValue().compareTo(kv1.getValue());
            	// If two values are not equal, sort the values in descending order (return the integer -1 or 1)
                if (compare != 0) {
                     return compare;
                } 
                // If two values are equal, sort the key in ascending order
                else {
                	 String skv1 = kv1.getKey().toString();
                	 String skv2 = kv2.getKey().toString();
                	 Integer kv11 = Integer.parseInt(skv1);
                	 Integer kv22 = Integer.parseInt(skv2);
                     return kv11.compareTo(kv22);
                }
            }
        }
        );
      
        // The linked list defines the iteration ordering, which is normally the order in which keys were inserted into the map (insertion-order)
        // This is useful since the keys is in natural ordering 
        Map<Text,IntWritable> sortedMap = new LinkedHashMap<Text,IntWritable>();
        
        // Put the list of key-value pairs into the Map
        for(Map.Entry<Text,IntWritable> kvPair: kvSet){
            sortedMap.put(kvPair.getKey(), kvPair.getValue());
        }
        // Return the map with sorted set of key-value pairs
        return sortedMap;
    }
	
     
}
