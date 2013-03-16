package com.payvment.continuuity;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import au.com.bytecode.opencsv.CSVParser;

import com.continuuity.api.annotation.Output;
import com.continuuity.api.annotation.ProcessInput;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import com.continuuity.api.flow.flowlet.StreamEvent;
import com.payvment.continuuity.data.ClusterTable;

/**
 * Flow application used to process clusters of categories.  These clusters
 * are used as an input to activity and popular feed queries.  A mapping from
 * a clusterid to a set of weighted categories is stored.
 * <p>
 * Activity and popular feeds are on a per-category basis, so to support cluster
 * queries, the clusters are queried and a query is made on each category in the
 * cluster and aggregated.
 * <p>
 * <b>Flow Design</b>
 * <p>
 *   <u>Input</u>
 *   <p>The input to this Flow is a stream named <i>clusters</i> which
 *   contains cluster events in the following CSV format:</p>
 *   <pre>
 *      clusterid,category,weight
 *   </pre>
 *   <blockquote>or when doing a reset, in the format:</blockquote>
 *   <pre>
 *      reset_clusters,max_cluster_id,"msg"
 *   </pre>
 * <p>
 *   <u>Flowlets</u>
 *   <p>This Flow is made up of three Flowlets.
 *   <p>The first flowlet, {@link ClusterSourceParser}, is responsible
 *   for parsing the cluster CSV line into the internal tuple representation and
 *   determining whether the operation is a WRITE or RESET.  The Flowlet will
 *   then send the Tuple to either the ClusterWriter or ClusterReset Flowlets.
 *   See {@link CLUSTER_PARSER_TO_WRITER_SCHEMA} and
 *   {@link CLUSTER_PARSER_TO_RESET_SCHEMA} for tuple schemas.
 *   <p>The Tuple is then passed on to one of the remaining two Flowlets,
 *    {@link ClusterWriter} and {@link ClusterReset}, where a cluster entry
 *    is written or all entries are cleared.
 *   <p>See the javadoc of each Flowlet class for more detailed information.
 * <p>
 *   <u>Tables</u>
 *   <p>This Flow utilizes one Tables.
 *   <p><i>clusterTable</i> is an instance of a {@link ClusterTable} used to
 *   store cluster information, a mapping from cluster id to categories and
 *   weights.  The primary key on this table is clusterId.
 */
public class ClusterWriterFlow implements Flow {

  /**
   * Name of this Flow.
   */
  public static final String FLOW_NAME = "ClusterWriter";

  public static final String PARSER_FLOWLET_NAME = "cluster_source_parser";
  public static final String WRITER_FLOWLET_NAME = "writer_output";
  public static final String RESET_FLOWLET_NAME = "reset_output";

  @Override
  public FlowSpecification configure() {
    return FlowSpecification.Builder.with()
        .setName(FLOW_NAME)
        .setDescription(FLOW_NAME)
        .withFlowlets()
          .add(PARSER_FLOWLET_NAME, new ClusterSourceParser())
          .add(WRITER_FLOWLET_NAME, new ClusterWriter())
          .add(RESET_FLOWLET_NAME, new ClusterReseter())
        .connect()
          .fromStream(LishApp.CLUSTER_STREAM).to(PARSER_FLOWLET_NAME)
          .from(PARSER_FLOWLET_NAME).to(WRITER_FLOWLET_NAME)
          .from(PARSER_FLOWLET_NAME).to(RESET_FLOWLET_NAME)
        .build();
  }

  public static class Cluster {
    public Integer clusterId;
    public String category;
    public Double weight;
    public Cluster(Integer clusterId, String category, Double weight) {
      this.clusterId = clusterId;
      this.category = category;
      this.weight = weight;
    }
  }

  public static class ClusterReset {
    public Integer maxClusterId;
    public String msg;
    public ClusterReset(Integer maxClusterId, String msg) {
      this.maxClusterId = maxClusterId;
      this.msg = msg;
    }
  }

  public static class ClusterSourceParser extends AbstractFlowlet {
    private static Logger LOG = LoggerFactory.getLogger(ClusterSourceParser.class);

    int numProcessed = 0;
    int numFailures = 0;

    private final CSVParser parser = new CSVParser(',', '"', '\\', false);

    @Output("cluster")
    OutputEmitter<Cluster> clusterOut;
    
    @Output("reset")
    OutputEmitter<ClusterReset> resetOut;

    @ProcessInput
    public void parseCluster(StreamEvent event) {
      // Grab CSV string from event-stream tuple
      String csvEventString = new String(Bytes.toBytes(event.getBody()));

      LOG.debug("ClusterSource Received Event: " + csvEventString);

      // Parse as CSV
      String[] parsed = null;
      try {
        parsed = this.parser.parseLine(csvEventString);
        if (parsed.length != 3) throw new IOException();
      } catch (IOException e) {
        LOG.error("Error parsing cluster CSV line: " + csvEventString);
        throw new RuntimeException("Invalid input string: " + csvEventString);
      }

      // Check if special flag to reset clusters exists
      if (parsed[0].equals(LishApp.CLUSTER_RESET_FLAG)) {
        // CSV = reset_clusters,max_cluster_id,"msg"

        LOG.debug("Received Cluster RESET");

        this.resetOut.emit(
            new ClusterReset(Integer.valueOf(parsed[1]), parsed[2]));
        numProcessed++;
        return;
      }

      // Format of CSV string is: clusterid,category,weight
      try {
        Cluster cluster = new Cluster(Integer.valueOf(parsed[0]), parsed[1],
            Double.valueOf(parsed[2]));

        this.clusterOut.emit(cluster);

      } catch (NumberFormatException nfe) {
        LOG.error("Error parsing numeric field in CSV line:" + csvEventString,
            nfe);
        throw nfe;
      }
      finally {
        this.numProcessed++;
      }
    }
  }
  
  /**
   * Flowlet that writes cluster entries to the cluster table in the data fabric.
   */
  public static class ClusterWriter extends AbstractFlowlet {
    private static Logger LOG = LoggerFactory.getLogger(ClusterWriter.class);

    int numProcessed = 0;

    @UseDataSet(LishApp.CLUSTER_TABLE)
    private ClusterTable clusterTable;

    @ProcessInput("cluster")
    public void writeCluster(Cluster cluster) throws OperationException {
      try {
        if (cluster == null) {
          LOG.error("Null cluster!");
          return;
        }
        this.clusterTable.writeCluster(cluster);
        LOG.debug("Writing cluster: " + cluster);
      } finally {
        this.numProcessed++;
      }
    }
  }

  /**
   * Flowlet that clears existing clusters from the data fabric.
   */
  public static class ClusterReseter extends AbstractFlowlet {
    private static Logger LOG = LoggerFactory.getLogger(ClusterReseter.class);

    int numProcessed = 0;

    @UseDataSet(LishApp.CLUSTER_TABLE)
    private ClusterTable clusterTable;

    @ProcessInput("reset")
    public void reset(ClusterReset reset) {
      try {
        if (reset == null) {
          LOG.error("Null cluster reset!");
          return;
        }
        LOG.info("Resetting clusters: " + reset);
        this.clusterTable.resetClusters(reset.maxClusterId);
      } catch (OperationException e) {
        LOG.error("Error resetting clusters", e);
        return;
      } finally {
        this.numProcessed++;
      }
    }
  }
}
