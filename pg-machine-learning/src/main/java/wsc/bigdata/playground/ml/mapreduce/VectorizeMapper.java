package wsc.bigdata.playground.ml.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * VectorizeMapper is the mapper that read each line and convert the line into VectorWritable
 */
public class VectorizeMapper extends Mapper<LongWritable, Text, LongWritable, VectorWritable> {
  private static final Logger logger = LoggerFactory.getLogger(VectorizeMapper.class);
  private static final int DEFAULT_NUM_TOKENS = 5;
  private int numberTokens;
  private String inputDelimiter;
  private String columnsSpec;
  private List<Integer> indexesList = null;

  public static enum VectorizeMapperCounters {
    NUM_LINE_READ, NUM_LINE_INVALID_NUM_TOKENS, NUM_LINE_VECTORIZED;
  }

  public void setup(Context context) throws IOException, InterruptedException {
    Configuration config = context.getConfiguration();
    numberTokens = config.getInt(VectorizeJob.Constants.KEY_NUMBER_TOKENS, DEFAULT_NUM_TOKENS);
    inputDelimiter = config.get(VectorizeJob.Constants.KEY_INPUT_DELIMITER);
    columnsSpec = config.get(VectorizeJob.Constants.KEY_COLUMNS_TO_PARSE);
    indexesList = initColumnsToParse(columnsSpec);
  }

  /**
   * Parse the columnsSpec and return the array of column indexes used.
   * @param columnsSpec
   * @return the array with column indexes (zero based) to be read
   */
  private List<Integer> initColumnsToParse(String columnsSpec) {
    List<Integer> indexesList = new ArrayList<Integer>();
    String[] groups = columnsSpec.split(",");
    for(String range: groups) {
      String[] ranges = range.split(":");
      int lowerBound = Integer.parseInt(ranges[0]);
      int upperBound = Integer.parseInt(ranges[1]);
      for(int i=lowerBound; i<upperBound+1; i++) {
        indexesList.add(i);
      }
    }
    return indexesList;
  }
  
  /* 
   * Parse each line and convert to VectorWritable, specifically NamedVector
   */
  public void map(LongWritable key, Text value, Context context) throws IOException,
      InterruptedException {
    String[] tokens = value.toString().split(inputDelimiter);
    if (tokens.length == numberTokens) {
      double[] elements = new double[indexesList.size()];
      int i = 0;
      for(int index: indexesList) {
        elements[i] = Double.parseDouble(tokens[index]);
        i++;
      }
      Vector vector = new NamedVector(new DenseVector(elements), tokens[0]);
      VectorWritable vectorWritable = new VectorWritable(vector);
      context.write(key, vectorWritable);
      context.getCounter(VectorizeMapperCounters.NUM_LINE_VECTORIZED).increment(1);
    }
    else {
      context.getCounter(VectorizeMapperCounters.NUM_LINE_INVALID_NUM_TOKENS).increment(1);
    }
  }
}
