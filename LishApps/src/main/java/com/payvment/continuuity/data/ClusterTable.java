package com.payvment.continuuity.data;

import java.util.Map;
import java.util.TreeMap;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.api.data.dataset.table.Delete;
import com.continuuity.api.data.dataset.table.Read;
import com.continuuity.api.data.dataset.table.Table;
import com.continuuity.api.data.dataset.table.Write;
import com.payvment.continuuity.ClusterWriterFlow.Cluster;

public class ClusterTable extends DataSet {

  private final Table table;

  public ClusterTable(String name) {
    super(name);
    this.table = new Table("cluster_" + name);
  }

  public ClusterTable(DataSetSpecification spec) {
    super(spec);
    this.table = new Table("cluster_" + this.getName());
  }

  @Override
  public DataSetSpecification configure() {
    return new DataSetSpecification.Builder(this)
        .dataset(this.table.configure())
        .create();
  }

  public void resetClusters(int maxClusterNumber) throws OperationException {
    for (int i=1;i<=maxClusterNumber;i++) {
      // short-term fix because Delete of a row does not work right now
      Map<String,Double> cluster = readCluster(i);
      if (cluster == null) return;
      byte [][] columns = new byte[cluster.size()][];
      int j = 0;
      for (String category : cluster.keySet()) {
        columns[j++] = Bytes.toBytes(category);
      }

      // Delete columns of row
      Delete delete = new Delete(makeRow(i), columns);
      this.table.write(delete);
    }
  }

  //
  // Fine grained schema
  //

  /**
   * Reads the cluster information for the specified cluster id.  Returns null
   * if no cluster information found.
   * <p>
   * This operation is synchronous.
   * @param clusterId
   * @return cluster info containing map from classification/category to weight
   * @throws OperationException
   */
  public Map<String,Double> readCluster(int clusterId)
      throws OperationException {
    OperationResult<Map<byte[],byte[]>> result =
        this.table.read(new Read(makeRow(clusterId)));
    if (result.isEmpty()) return null;
    Map<byte[],byte[]> map = result.getValue();
    Map<String,Double> ret = new TreeMap<String,Double>();
    for (Map.Entry<byte[],byte[]> entry : map.entrySet()) {
      ret.put(Bytes.toString(entry.getKey()), Bytes.toDouble(entry.getValue()));
    }
    return ret;
  }

  /**
   * Writes the specified cluster to the table.
   * <p>
   * Updates any existing weight for this category and cluster.
   * <p>
   * This operation is asynchronous and will be performed as part of the flowlet
   * process batch.
   * @param clusterId
   * @param category
   * @param weight
   */
  public void writeCluster(Cluster cluster) throws OperationException {
    this.table.write(new Write(makeRow(cluster.clusterId),
        Bytes.toBytes(cluster.category), Bytes.toBytes(cluster.weight)));
  }

  public static byte [] makeRow(int clusterid) {
    return Bytes.toBytes(clusterid);
  }
}
