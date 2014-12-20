package wsc.bigdata.playground.ml.mapreduce;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.mahout.clustering.classify.WeightedPropertyVectorWritable;
import org.apache.mahout.math.NamedVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class lists the members of each cluster into an output file given the clustering output
 * 
 */
public class ClusterMembershipInfo {
  private static final Logger logger = LoggerFactory.getLogger(ClusterMembershipInfo.class);
  
  /**
   * Map of cluster id and list of cluster member ids
   */
  private Map<Integer, List<String>> clusterInfo;

  public ClusterMembershipInfo() {
    clusterInfo = new TreeMap<Integer, List<String>>();
  }

  /**
   * @return the number of clusters
   */
  public int getNumberClusters() {
    return clusterInfo.size();
  }

  /**
   * Return a map of cluster id and its associated list of members
   * 
   * @param clusterPath the path of clustered points
   * @return the map of cluster id and the associated list of members in the cluster
   * @throws IOException
   */
  public Map<Integer, List<String>> generateClusterInfo(String clusterPath) throws IOException {
    Path path = new Path(clusterPath);
    Configuration config = new Configuration();
    SequenceFile.Reader reader = new SequenceFile.Reader(config, SequenceFile.Reader.file(path));

    IntWritable clusterIdWritable = new IntWritable();
    WeightedPropertyVectorWritable vectorWritable = new WeightedPropertyVectorWritable();

    while (reader.next(clusterIdWritable, vectorWritable)) {
      NamedVector vector = (NamedVector) vectorWritable.getVector();
      int clusterId = clusterIdWritable.get();
      if (clusterInfo.containsKey(clusterId)) {
        clusterInfo.get(clusterId).add(vector.getName());
      } else {
        List<String> list = new ArrayList<String>();
        list.add(vector.getName());
        clusterInfo.put(clusterId, list);
      }
    }
    return clusterInfo;
  }

  /**
   * Write the cluster membership to an output file
   * 
   * @param outputFile the output file
   */
  public void write(String outputFile) {
    BufferedWriter writer = null;
    try {
      writer = new BufferedWriter(new FileWriter(new File(outputFile)));
      for (Map.Entry<Integer, List<String>> entry : clusterInfo.entrySet()) {
        List<String> list = entry.getValue();
        writer.write(entry.getKey() + ":");
        int i = 0;
        for (String elem : list) {
          if (i < list.size() - 1) {
            writer.write(elem + ",");
          } else {
            writer.write(elem);
            writer.newLine();
          }
          i++;
        }
      }
    } catch (IOException e) {
      logger.info("Error in writing cluster info", e);
    } finally {
      IOUtils.closeQuietly(writer);
    }
  }

  /**
   * The program expects two arguments, the first argument is the path of clustered points 
   * and the second is the output file
   * @param args the arguments to program
   * @throws IOException
   */
  public static void main(String[] args) throws IOException {
    String clusterPath = args[0];
    String outputFile = args[1];
    ClusterMembershipInfo clusterMembership = new ClusterMembershipInfo();
    clusterMembership.generateClusterInfo(clusterPath);
    clusterMembership.write(outputFile);
  }
}
