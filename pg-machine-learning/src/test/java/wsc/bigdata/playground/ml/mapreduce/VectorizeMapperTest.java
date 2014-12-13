package wsc.bigdata.playground.ml.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.VectorWritable;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class VectorizeMapperTest {
  private static final Logger logger = LoggerFactory.getLogger(VectorizeMapperTest.class);
  MapDriver<LongWritable, Text, LongWritable, VectorWritable> mapDriver;
  VectorizeMapper mapper = new VectorizeMapper();

  @Before
  public void setUp() throws Exception {
    mapDriver = new MapDriver<LongWritable, Text, LongWritable, VectorWritable>();
    Configuration config = new Configuration();
    config.set(VectorizeJob.Constants.KEY_INPUT_DELIMITER, ",");
    config.set(VectorizeJob.Constants.KEY_COLUMNS_TO_PARSE, "1:6");
    config.setInt(VectorizeJob.Constants.KEY_NUMBER_TOKENS, 7);
    mapDriver.setConfiguration(config);
    mapDriver.setMapper(mapper);
  }

  @Test
  public void testVectorize() throws IOException {
    double[] values1 = new double[] {5, 2, 3, 1, 1, 1};
    double[] values2 = new double[] {1, 2, 3, 1, 1, 1};
    mapDriver.withInput(new LongWritable(1), new Text("ab1,5,2,3,1,1,1"));
    mapDriver.withInput(new LongWritable(2), new Text("ab1,1,2,3,1,1,1"));
    mapDriver.withOutput(new LongWritable(1), new VectorWritable(new NamedVector(new DenseVector(values1), "ab1")));
    mapDriver.withOutput(new LongWritable(2), new VectorWritable(new NamedVector(new DenseVector(values2), "ab1")));
    mapDriver.runTest();
  }
}
