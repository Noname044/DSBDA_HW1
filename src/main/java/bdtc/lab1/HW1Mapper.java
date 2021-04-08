package bdtc.lab1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.regex.Pattern;

/**
 * Mapper class
 * Input key {@link LongWritable}
 * Input value {@link Text}
 * Output key {@link Text}
 * Output value {@link CustomInt}
 */
public class HW1Mapper extends Mapper<LongWritable, Text, Text, CustomInt> {

    /**
     * Regex pattern to check if the input row is correct
     */
    private final static Pattern regular = Pattern.compile("^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},[0-7]$");

    /**
     * Regex pattern to split input row
     */
    private final static Pattern splitter = Pattern.compile(":|,");

    /**
     * Map function. Checks data for correctness and determines the hours and types of errors
     * Uses counters {@link CounterType}
     * @param key input key
     * @param value input value
     * @param context mapper context
     * @throws IOException from context.write()
     * @throws InterruptedException from context.write()
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (regular.matcher(line).matches()) {
            String[] parts = splitter.split(line);
            context.write(new Text(parts[0]), new CustomInt(Integer.parseInt(parts[3])));
        }
        else {
            context.getCounter(CounterType.MALFORMED).increment(1);
        }
    }
}