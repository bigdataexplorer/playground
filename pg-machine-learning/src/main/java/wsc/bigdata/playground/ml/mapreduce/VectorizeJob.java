package wsc.bigdata.playground.ml.mapreduce;

import java.io.IOException;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This mapreduce job read the input and convert them into vectors
 *
 */
public class VectorizeJob extends Configured implements Tool {
  private static final Logger logger = LoggerFactory.getLogger(VectorizeJob.class);

  public static final class Constants {
    public static final String KEY_INPUT_PATH = "input.path";
    public static final String KEY_OUTPUT_PATH = "output.path";
    public static final String KEY_INPUT_DELIMITER = "input.delimiter";
    public static final String KEY_OUTPUT_DELIMITER = "output.delimiter";
    public static final String KEY_COLUMNS_TO_PARSE = "columns";
    public static final String KEY_NUMBER_TOKENS = "number.tokens";
  }

  public static VectorizeJobOptions loadArgs(String[] args) {
    VectorizeJobOptions options = new VectorizeJobOptions();
    try {
      options.populate(args);
    } catch (ParseException e) {
      logger.error("Error parsing arguments", e);
      options.usageExit("VectorizeJob");
    }
    return options;
  }
  
  /*
   * Setup MapReduce job
   */
  @Override
  public int run(String[] args) throws Exception {
    Configuration config = new Configuration();
    String[] jobArgs = new GenericOptionsParser(config, args).getRemainingArgs();
    VectorizeJobOptions options = loadArgs(jobArgs);
   
    Path inputPath = new Path(options.getInputPath());
    Path outputPath = new Path(options.getOutputPath());
    delete(config, outputPath);
    
    Job job = new Job(config, "VectorizeJob");
    job.setJarByClass(VectorizeJob.class);
    Configuration mrConfig = job.getConfiguration();

    mrConfig.set(Constants.KEY_INPUT_DELIMITER, options.getInputDelimiter());
    mrConfig.setInt(Constants.KEY_NUMBER_TOKENS, options.getNumberTokens());
    mrConfig.set(Constants.KEY_COLUMNS_TO_PARSE, options.getColumns());
    mrConfig.set(Constants.KEY_OUTPUT_DELIMITER, options.getOutputDelimiter());
    
    job.setMapperClass(VectorizeMapper.class);
    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(VectorWritable.class);
    
    FileOutputFormat.setOutputPath(job, outputPath);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
   
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(VectorWritable.class);
    job.setNumReduceTasks(0);
    FileInputFormat.setInputPaths(job, inputPath);
    return job.waitForCompletion(true) ? 0 : 1;
  }
  
  /**
   * Delete output path if the path exists
   * @param config the job config
   * @param path the output path
   * @throws IOException
   */
  public void delete(Configuration config, Path path) throws IOException {
    FileSystem fs = path.getFileSystem(config);
    if(fs.exists(path)) {
      fs.delete(path, true);
    }
  }
  
  public static void main(String[] args) throws Exception {
    int status = ToolRunner.run(new Configuration(), new VectorizeJob(), args);
    System.exit(status);
  }
}
