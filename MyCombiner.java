
//Part 1 - Find most active users
package part1;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class MyCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
	private IntWritable SUM = new IntWritable();
	
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
		SUM.set(sum);
		// write (user, total number of revision made by this user)
		// pass the total count to Reducer
		context.write(new Text(key), SUM);
	}

}

