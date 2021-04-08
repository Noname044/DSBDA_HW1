package bdtc.lab1;

import lombok.extern.log4j.Log4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.io.SequenceFile;

@Log4j
public class MapReduceApplication {

    /**
     * Entry point for the application
     *
     * @param args Optional arguments: InputDirectory, OutputDirectory
     * @throws Exception when error occurred
     */
    public static void main(String[] args) throws Exception {

        if (args.length < 2) {
            throw new RuntimeException("You should specify input and output folders!");
        }
        Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", ",");

        Job job = Job.getInstance(conf, "error by hour");
        job.setJarByClass(MapReduceApplication.class);
        job.setMapperClass(HW1Mapper.class);
        job.setReducerClass(HW1Reducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(CustomInt.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
        SequenceFileOutputFormat.setCompressOutput(job, true);
        SequenceFileOutputFormat.setOutputCompressorClass(job, org.apache.hadoop.io.compress.SnappyCodec.class);/*Sequence file compress by snappy*/

        Path outputDirectory = new Path(args[1]);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, outputDirectory);
        log.info("=====================JOB STARTED=====================");
        job.waitForCompletion(true);
        log.info("=====================JOB ENDED=====================");
        // проверяем статистику по счётчикам
        Counter counter = job.getCounters().findCounter(CounterType.MALFORMED);
        log.info("=====================COUNTERS " + counter.getName() + ": " + counter.getValue() + "=====================");
    }
}
