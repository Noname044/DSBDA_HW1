import bdtc.lab1.CustomInt;
import bdtc.lab1.HW1Mapper;
import bdtc.lab1.HW1Reducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class MapReduceTest {

    private MapDriver<LongWritable, Text, Text, CustomInt> mapDriver;
    private ReduceDriver<Text, CustomInt, Text, Text> reduceDriver;
    private MapReduceDriver<LongWritable, Text, Text, CustomInt, Text, Text> mapReduceDriver;

    private final String testDataReducer = "2021-03-24 18";
    private final String testDataMapper = "2021-03-24 18:23:00,1";

    @Before
    public void setUp() {
        HW1Mapper mapper = new HW1Mapper();
        HW1Reducer reducer = new HW1Reducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapReduce() throws IOException {
        HashMap <String, Integer> statistic = new HashMap<>();
        statistic.put("alert", 1);
        mapReduceDriver
                .withInput(new LongWritable(), new Text(testDataMapper))
                .withOutput(new Text(testDataReducer), new Text(statistic.toString()))
                .runTest();
    }

    @Test
    public void testDataReducer() throws IOException {
        List<CustomInt> values = new ArrayList<>();
        values.add(new CustomInt(7));
        values.add(new CustomInt(7));
        values.add(new CustomInt(7));
        values.add(new CustomInt(2));
        HashMap <String, Integer> statistic = new HashMap<>();
        statistic.put("debug", 3);
        statistic.put("crit", 1);
        reduceDriver
                .withInput(new Text(testDataReducer), values)
                .withOutput(new Text(testDataReducer), new Text(statistic.toString()))
                .runTest();
    }

    @Test
    public void testDataMapper() throws IOException {
        mapDriver
                .withInput(new LongWritable(), new Text(testDataMapper))
                .withOutput(new Text(testDataReducer), new CustomInt(1))
                .runTest();
    }

}