package com.payvment.continuuity;

import au.com.bytecode.opencsv.CSVParser;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecifier;
import com.continuuity.api.flow.flowlet.ComputeFlowlet;
import com.continuuity.api.flow.flowlet.FailureHandlingPolicy;
import com.continuuity.api.flow.flowlet.FailureReason;
import com.continuuity.api.flow.flowlet.FlowletSpecifier;
import com.continuuity.api.flow.flowlet.OutputCollector;
import com.continuuity.api.flow.flowlet.Tuple;
import com.continuuity.api.flow.flowlet.TupleContext;
import com.continuuity.api.flow.flowlet.TupleSchema;
import com.continuuity.api.flow.flowlet.builders.TupleBuilder;
import com.continuuity.api.flow.flowlet.builders.TupleSchemaBuilder;
import com.payvment.continuuity.data.ClusterTable;

import java.io.IOException;

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
   * Name of the input stream carrying CSV Payvment generated clusters.
   */
  public static final String inputStream = "clusters";

  /**
   * Name of this Flow.
   */
  public static final String flowName = "ClusterWriter";

  @Override
  public void configure(FlowSpecifier specifier) {

    // Set metadata fields
    specifier.name(flowName);
    specifier.email("dev@continuuity.com");
    specifier.application("ClusterFeeds");
    specifier.company("Continuuity+Payvment");

    // Declare all of the flowlets within the flow
    specifier.flowlet("cluster_source_parser", ClusterSourceParser.class, 1);
    specifier.flowlet("cluster_writer", ClusterWriter.class, 1);
    specifier.flowlet("cluster_reset", ClusterReset.class, 1);

    // Define clusters stream and connect to cluster_source_parser
    specifier.stream(inputStream);
    specifier.input(inputStream, "cluster_source_parser");

    // Wire up the remaining flowlet connections
    specifier.connection("cluster_source_parser", "writer_output",
        "cluster_writer", "in");
    specifier.connection("cluster_source_parser", "reset_output",
        "cluster_reset", "in");
  }

  /**
   * Reads raw events from the cluster input stream and parses the input CSV
   * into a tuple that will be written in the next flowlet.
   */
  public static class ClusterSourceParser extends ComputeFlowlet {

    static int numProcessed = 0;

    static int numFailures = 0;

    @Override
    public void configure(FlowletSpecifier configurator) {

      // Apply default stream schema to default tuple input stream
      configurator
          .getDefaultFlowletInput()
          .setSchema(TupleSchema.EVENT_SCHEMA);

      // Apply internal cluster tuple schema to writer output stream
      configurator
        .addFlowletOutput("writer_output")
        .setSchema(CLUSTER_PARSER_TO_WRITER_SCHEMA);

      // Apply simple reset tuple schema to reset output stream
      configurator
          .addFlowletOutput("reset_output")
          .setSchema(CLUSTER_PARSER_TO_RESET_SCHEMA);

    }

    private final CSVParser parser = new CSVParser(',', '"', '\\', false);

    @Override
    public void process(Tuple tuple, TupleContext context,
        OutputCollector collector) {

      // Grab CSV string from event-stream tuple
      String csvEventString = new String((byte[])tuple.get("body"));

      getFlowletContext().getLogger().debug(
          "ClusterSource Received Event: " + csvEventString);

      // Parse as CSV
      String [] parsed = null;
      try {
        parsed = this.parser.parseLine(csvEventString);
        if (parsed.length != 3) throw new IOException();
      } catch (IOException e) {
        getFlowletContext().getLogger().error(
            "Error parsing cluster CSV line: " + csvEventString);
        throw new RuntimeException("Invalid input string: " + csvEventString);
      }

      // Check if special flag to reset clusters exists
      if (parsed[0].equals(Constants.CLUSTER_RESET_FLAG)) {
        // CSV = reset_clusters,max_cluster_id,"msg"
        getFlowletContext().getLogger().debug("Received Cluster RESET");
        Tuple resetTuple = new TupleBuilder()
            .set("maxClusterId", Integer.valueOf(parsed[1]))
            .set("msg", parsed[2])
            .create();
        collector.add("reset_output", resetTuple);
        return;
      }

      // Format of CSV string is: clusterid,category,weight
      try {
        Tuple clusterTuple = new TupleBuilder()
            .set("clusterId", Integer.valueOf(parsed[0]))
            .set("category", parsed[1])
            .set("weight", Double.valueOf(parsed[2]))
            .create();
        collector.add("writer_output", clusterTuple);
      } catch (NumberFormatException nfe) {
        getFlowletContext().getLogger().error(
            "Error parsing numeric field in CSV line:" + csvEventString, nfe);
        throw nfe;
      }
    }

    @Override
    public void onSuccess(Tuple tuple, TupleContext context) {
      numProcessed++;
    }

    @Override
    public FailureHandlingPolicy onFailure(Tuple tuple, TupleContext context,
        FailureReason reason) {
      numFailures++;
      getFlowletContext().getLogger().error(
          "ClusterSource Flowet Failed : " + reason.toString() +
          ", retrying");
      return FailureHandlingPolicy.RETRY;
    }

  }

  public static final TupleSchema CLUSTER_PARSER_TO_WRITER_SCHEMA =
      new TupleSchemaBuilder()
          .add("clusterId", Integer.class)
          .add("category", String.class)
          .add("weight", Double.class)
          .create();

  public static final TupleSchema CLUSTER_PARSER_TO_RESET_SCHEMA =
      new TupleSchemaBuilder()
          .add("maxClusterId", Integer.class)
          .add("msg", String.class)
          .create();

  /**
   * Flowlet that writes cluster entries to the cluster table in the data fabric.
   */
  public static class ClusterWriter extends ComputeFlowlet {

    static int numProcessed = 0;

    @Override
    public void configure(FlowletSpecifier configurator) {
      // Apply cluster tuple schema to default tuple input stream
      configurator
          .getDefaultFlowletInput()
          .setSchema(CLUSTER_PARSER_TO_WRITER_SCHEMA);
      // No output stream
    }

    private ClusterTable clusterTable;

    @Override
    public void initialize() {
      this.clusterTable = new ClusterTable();
      getFlowletContext().getDataSetRegistry().registerDataSet(
          this.clusterTable);
    }

    @Override
    public void process(Tuple tuple, TupleContext context,
        OutputCollector collector) {
      Integer clusterId = tuple.get("clusterId");
      String category = tuple.get("category");
      Double weight = tuple.get("weight");
      this.clusterTable.writeCluster(clusterId, category, weight);
      getFlowletContext().getLogger().debug(
          "Writing cluster (id=" + clusterId + ", category=" +
              category + ", weight=" + weight);
    }

    @Override
    public void onSuccess(Tuple tuple, TupleContext context) {
      numProcessed++;
    }

    @Override
    public FailureHandlingPolicy onFailure(Tuple tuple, TupleContext context,
        FailureReason reason) {
      getFlowletContext().getLogger().error(
          "ClusterWriter Flowet Processing Failed : " +
              reason.toString() + ", retrying");
      return FailureHandlingPolicy.RETRY;
    }

  }

  /**
   * Flowlet that clears existing clusters from the data fabric.
   */
  public static class ClusterReset extends ComputeFlowlet {

    static int numProcessed = 0;

    @Override
    public void configure(FlowletSpecifier configurator) {
      // Apply cluster tuple schema to default tuple input stream
      configurator
          .getDefaultFlowletInput()
          .setSchema(CLUSTER_PARSER_TO_RESET_SCHEMA);
      // No output stream
    }

    private ClusterTable clusterTable;

    @Override
    public void initialize() {
      this.clusterTable = new ClusterTable();
      getFlowletContext().getDataSetRegistry().registerDataSet(
          this.clusterTable);
    }

    @Override
    public void process(Tuple tuple, TupleContext context,
        OutputCollector collector) {
      Integer maxClusterId = tuple.get("maxClusterId");
      try {
        this.clusterTable.resetClusters(maxClusterId);
      } catch (OperationException e) {
        getFlowletContext().getLogger().error("Error resetting clusters", e);
        return;
      }
      getFlowletContext().getLogger().info(
          "Resetting clusters using maxId " + maxClusterId);
    }

    @Override
    public void onSuccess(Tuple tuple, TupleContext context) {
      numProcessed++;
    }

    @Override
    public FailureHandlingPolicy onFailure(Tuple tuple, TupleContext context,
        FailureReason reason) {
      getFlowletContext().getLogger().error(
          "ClusterReset Flowet Processing Failed : " + reason.toString() +
          ", retrying");
      return FailureHandlingPolicy.RETRY;
    }

  }
}
