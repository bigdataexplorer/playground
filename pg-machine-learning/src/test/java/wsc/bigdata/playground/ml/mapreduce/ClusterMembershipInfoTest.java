package wsc.bigdata.playground.ml.mapreduce;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.mahout.clustering.classify.WeightedPropertyVectorWritable;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.NamedVector;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ClusterMembershipInfoTest {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();
  public Path seqFilePath = null;
  public List<double[]> vectors;

  /**
   * generate list of vectors
   */
  private void generateVectors() {
    vectors = new ArrayList<double[]>();
    int numVectors = 10;
    int numElements = 10;
    Random rand = new Random();
    for (int i = 0; i < numVectors; i++) {

      double[] array = new double[numElements];
      for (int j = 0; j < numElements; j++) {
        array[j] = rand.nextDouble();
      }
      vectors.add(array);
    }
  }

  /**
   * Write sequence file of clustered points. There are two clusters, 
   * cluster 0 with array0,array2,array4,array6,array8
   * cluster 1 with array1,array3,array5,array7,array9
   * @throws Exception
   */
  @Before
  public void setUp() throws Exception {
    Configuration conf = new Configuration();
    seqFilePath = new Path(temp.newFile().getAbsolutePath());
    SequenceFile.Writer writer =
        SequenceFile.createWriter(conf, Writer.file(seqFilePath),
            Writer.keyClass(IntWritable.class),
            Writer.valueClass(WeightedPropertyVectorWritable.class));
    generateVectors();
    int id = 0;
    for (double[] array : vectors) {
      double weight = 0.8;
      Map<Text, Text> map = new HashMap<Text, Text>();
      map.put(new Text("distance"), new Text("1.13"));
      WeightedPropertyVectorWritable vectorWritable =
          new WeightedPropertyVectorWritable(weight, new NamedVector(new DenseVector(array),
              "array" + id), map);
      if (id % 2 == 0) {
        writer.append(new IntWritable(0), vectorWritable);
      } else {
        writer.append(new IntWritable(1), vectorWritable);
      }
      id++;
    }
    writer.close();
  }

  /**
   * Test number of clusters after reading the clustered points file
   * @throws IOException
   */
  @Test
  public void testGetNumberClusters() throws IOException {
    String clusterPath = seqFilePath.toString();
    ClusterMembershipInfo clusterMembership = new ClusterMembershipInfo();
    clusterMembership.generateClusterInfo(clusterPath);

    assertEquals(2, clusterMembership.getNumberClusters());
  }

  /**
   * Test cluster members for each cluster
   * @throws IOException
   */
  @Test
  public void testGenerateClusterInfo() throws IOException {
    String clusterPath = seqFilePath.toString();
    ClusterMembershipInfo clusterMembership = new ClusterMembershipInfo();
    Map<Integer, List<String>> clusterInfo = clusterMembership.generateClusterInfo(clusterPath);

    int id = 0;
    for (Map.Entry<Integer, List<String>> entry : clusterInfo.entrySet()) {
      List<String> list = entry.getValue();
      assertEquals(id, entry.getKey().intValue());

      int i = 0, suffix = 0;
      for (String elem : list) {
        if (id == 0) {
          suffix = i * 2;
        } else {
          suffix = i * 2 + 1;
        }
        assertEquals("array" + suffix, elem);
        i++;
      }
      id++;
    }
  }
}
