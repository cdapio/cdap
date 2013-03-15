package com.payvment.continuuity.data;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

//import com.continuuity.api.data.DataLib;
import com.continuuity.api.data.DataSet;
//import com.continuuity.api.data.Delete;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
//import com.continuuity.api.data.ReadColumnRange;

//import com.continuuity.api.data.Write;
//import com.continuuity.api.data.util.Bytes;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.dataset.table.Read;
import com.continuuity.api.data.dataset.table.Write;
import com.continuuity.api.data.dataset.table.Delete;


import com.continuuity.api.data.dataset.table.Table;
import com.google.common.base.Throwables;

public class ClusterTable extends DataSet {

  private static final String CLUSTER_TABLE = "ClusterTable";


  private Table table;

  public ClusterTable(String name) {
      super(name);
      table = new Table("cluster_" + this.getName());
  }


    public DataSetSpecification configure() {
        return null;
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

      // Delete rows
      Delete delete = new Delete(makeRow(i), columns);
      this.table.write(delete);

        // Old logic
      //getCollector().add(new Delete(getDataSetId(), makeRow(i), columns));


      // TODO: Use the below delete once row deletes are working
      // getCollector().add(new Delete(getDataSetId(), makeRow(i)));
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




    // Old logic
//   OperationResult<Map<byte[],byte[]>> result =
//      getDataFabric().read(new ReadColumnRange(getDataSetId(), makeRow(clusterId), null));

   Read read = new Read(makeRow(clusterId));
   OperationResult<Map<byte[],byte[]>> result  = table.read( new Read(makeRow(clusterId)) );


    if (result.isEmpty()) return null;
    Map<byte[],byte[]> map = result.getValue();
    Map<String,Double> ret = new TreeMap<String,Double>();
    for (Map.Entry<byte[],byte[]> entry : map.entrySet()) {
      ret.put(Bytes.toString(entry.getKey()), Bytes.toDouble(entry.getValue()));
    }
    return ret;
  }

  /**
   * Writes the specified cluster information for the specified cluster id.
   * <p>
   * This operation is asynchronous and will be performed as part of the flowlet
   * process batch.
   * @param clusterId id of cluster
   * @param clusterInfo map of category name to weight in cluster
   */
  public void writeCluster(int clusterId, Map<String,Double> clusterInfo) {
    int len = clusterInfo.size();
    List<byte[]> strings = new ArrayList<byte[]>(len);
    List<byte[]> doubles = new ArrayList<byte[]>(len);
    for (Map.Entry<String,Double> info : clusterInfo.entrySet()) {
      strings.add(Bytes.toBytes(info.getKey()));
      doubles.add(Bytes.toBytes(info.getValue().doubleValue()));
    }

    // Old logic
//    getCollector().add(new Write(getDataSetId(), makeRow(clusterId),
//        strings.toArray(new byte[len][]), doubles.toArray(new byte[len][])));

      try {
      /* row, columns, values*/
      Write write = new Write(makeRow(clusterId), strings.toArray(new byte[len][]), doubles.toArray(new byte[len][]));
      table.write(write);
      } catch (OperationException e) {
          throw Throwables.propagate(e);
      }

  }

  /**
   * Writes the specified category and weight into the specified cluster.
   * <p>
   * Updates any existing weight for this category and cluster.
   * <p>
   * This operation is asynchronous and will be performed as part of the flowlet
   * process batch.
   * @param clusterId
   * @param category
   * @param weight
   */
  public void writeCluster(int clusterId, String category, Double weight) {

// old logic
//    getCollector().add(new Write(getDataSetId(), makeRow(clusterId),
//        Bytes.toBytes(category), Bytes.toBytes(weight)));
  }

  //
  // Coarse grained schema (keep switching between this and above)
  //
  //  private static final byte [] COLUMN = Bytes.toBytes("c");
  //
  //  /**
  //   * Reads the cluster information for the specified cluster id.  Returns null
  //   * if no cluster information found.
  //   * <p>
  //   * This operation is synchronous.
  //   * @param clusterId
  //   * @return cluster info containing map from classification/category to weight
  //   * @throws OperationException
  //   */
  //  public Map<String,Double> readCluster(int clusterId)
  //      throws OperationException {
  //    OperationResult<Map<byte[],byte[]>> result =
  //        this.fabric.read(new Read(makeRow(clusterId), COLUMN));
  //    if (result.isEmpty()) return null;
  //    Map<byte[],byte[]> map = result.getValue();
  //    Cluster cluster = Cluster.fromBytes(map.get(COLUMN));
  //    return cluster.getClusterInfo();
  //  }
  //
  //  /**
  //   * Writes the specified cluster information for the specified cluster id.
  //   * <p>
  //   * This operation is asynchronous and will be performed as part of a flowlet
  //   * process batch.
  //   * @param clusterid
  //   * @param clusterInfo
  //   */
  //  public void writeCluster(int clusterId, Map<String,Double> clusterInfo) {
  //    this.collector.add(new Write(makeRow(clusterId), COLUMN,
  //        new Cluster(clusterId, clusterInfo).toBytes()));
  //  }
  
  public static byte [] makeRow(int clusterid) {
    return Bytes.add(Bytes.toBytes(CLUSTER_TABLE), Bytes.toBytes(clusterid));
  }
}
