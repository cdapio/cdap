package com.payvment.continuuity;

import java.io.IOException;
import au.com.bytecode.opencsv.CSVParser;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;
import com.payvment.continuuity.data.ClusterTable;
import com.continuuity.api.annotation.*;
import com.continuuity.api.flow.flowlet.*;

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
    public static final String flowName = "ClusterWriter";

    public static final String cluster_source_parser = "cluster_source_parser";
    public static final String writer_output = "writer_output";
    public static final String reset_output = "reset_output";


    @Override
    public FlowSpecification configure() {
        return FlowSpecification.Builder.with()
                .setName(flowName)
                .setDescription("Continuuity+Payvment:ClusterWriter")
                .withFlowlets()
                .add(cluster_source_parser, new ClusterSourceParser())
                .add(writer_output, new ClusterWriter())
                .add(reset_output, new ClusterReset())
                .connect()

                .fromStream(LishApp.inputStream).to(cluster_source_parser)
                .from(cluster_source_parser).to(writer_output)
                .from(cluster_source_parser).to(reset_output)
                .build();
    }

//  @Override
//  public void configure(FlowSpecifier specifier) {
//
//    // Set metadata fields
//    specifier.name(flowName);
//    specifier.email("dev@continuuity.com");
//    specifier.application("ClusterFeeds");
//    specifier.company("Continuuity+Payvment");
//
//    // Declare all of the flowlets within the flow
//    specifier.flowlet("cluster_source_parser", ClusterSourceParser.class, 1);
//    specifier.flowlet("cluster_writer", ClusterWriter.class, 1);
//    specifier.flowlet("cluster_reset", ClusterReset.class, 1);
//
//    // Define clusters stream and connect to cluster_source_parser
//    specifier.stream(inputStream);
//    specifier.input(inputStream, "cluster_source_parser");
//
//    // Wire up the remaining flowlet connections
//    specifier.connection("cluster_source_parser", "writer_output",
//        "cluster_writer", "in");
//    specifier.connection("cluster_source_parser", "reset_output",
//        "cluster_reset", "in");
//  }


    public class ClusterSourceParser extends AbstractFlowlet {


        int numProcessed = 0;
        int numFailures = 0;

        private final CSVParser parser = new CSVParser(',', '"', '\\', false);

        OutputEmitter<CLUSTER_PARSER_TO_WRITER_SCHEMA> parserToWriterEmitter;
        OutputEmitter<CLUSTER_PARSER_TO_RESET_SCHEMA> parserToResetEmitter;

        @Override
        public void initialize(FlowletContext context) throws FlowletException {

        }

        @Override
        public FlowletSpecification configure() {
            return FlowletSpecification.Builder.with()
                    .setName(ClusterWriterFlow.cluster_source_parser)
                    .setDescription("")
                    .build();
        }

        @Override
        public void destroy() {
            // TODO Auto-generated method stub
        }

        @ProcessInput(LishApp.inputStream)
        public void process(StreamEvent event) {
            //      // Grab CSV string from event-stream tuple
            String csvEventString = new String(Bytes.toBytes(event.getBody()));

            // TODO: add logging
            // getFlowletContext().getLogger().debug(
            //   "ClusterSource Received Event: " + csvEventString);

            // Parse as CSV
            String[] parsed = null;
            try {
                parsed = this.parser.parseLine(csvEventString);
                if (parsed.length != 3) throw new IOException();
            } catch (IOException e) {

                // TODO: Add logging back
                //getFlowletContext().getLogger().error(
                //        "Error parsing cluster CSV line: " + csvEventString);

                throw new RuntimeException("Invalid input string: " + csvEventString);
            }

            // Check if special flag to reset clusters exists
            if (parsed[0].equals(Constants.CLUSTER_RESET_FLAG)) {
                // CSV = reset_clusters,max_cluster_id,"msg"

                // TODO: Add logging back
                //getFlowletContext().getLogger().debug("Received Cluster RESET");

                ClusterWriterFlow.CLUSTER_PARSER_TO_RESET_SCHEMA resetTuple = new ClusterWriterFlow.CLUSTER_PARSER_TO_RESET_SCHEMA();
                resetTuple.maxClusterId = Integer.valueOf(parsed[1]);
                resetTuple.msg = parsed[2];

                parserToResetEmitter.emit(resetTuple);
            }

            // Format of CSV string is: clusterid,category,weight
            try {
                ClusterWriterFlow.CLUSTER_PARSER_TO_WRITER_SCHEMA clusterTuple = new ClusterWriterFlow.CLUSTER_PARSER_TO_WRITER_SCHEMA();
                clusterTuple.clusterId =  Integer.valueOf(parsed[0]);
                clusterTuple.category =  parsed[1];
                clusterTuple.weight = Double.valueOf(parsed[2]);

                parserToWriterEmitter.emit(clusterTuple);

            } catch (NumberFormatException nfe) {

                // TODO: add logging back
                //getFlowletContext().getLogger().error(
                //        "Error parsing numeric field in CSV line:" + csvEventString, nfe);
                throw nfe;
            }
            finally {
                this.numProcessed++;
            }
        }
    }

    public class CLUSTER_PARSER_TO_WRITER_SCHEMA {
        public Integer clusterId;
        public String category;
        public Double weight;

        CLUSTER_PARSER_TO_WRITER_SCHEMA() {
             clusterId =0;
             category = "";
             weight =0.0;
        }
    }

//  public static final TupleSchema CLUSTER_PARSER_TO_WRITER_SCHEMA =
//      new TupleSchemaBuilder()
//          .add("clusterId", Integer.class)
//          .add("category", String.class)
//          .add("weight", Double.class)
//          .create();


  public class CLUSTER_PARSER_TO_RESET_SCHEMA {
      public Integer maxClusterId;
      public String msg;

      CLUSTER_PARSER_TO_RESET_SCHEMA() {
         maxClusterId = 0;
          msg = "";
      }

  }

//  public static final TupleSchema CLUSTER_PARSER_TO_RESET_SCHEMA =
//      new TupleSchemaBuilder()
//          .add("maxClusterId", Integer.class)
//          .add("msg", String.class)
//          .create();

  /**
   * Flowlet that writes cluster entries to the cluster table in the data fabric.
   */
  public class ClusterWriter extends AbstractFlowlet {

    int numProcessed = 0;

//    @Override
//    public void configure(StreamsConfigurator configurator) {
//      // Apply cluster tuple schema to default tuple input stream
//      configurator
//          .getDefaultTupleInputStream()
//          .setSchema(CLUSTER_PARSER_TO_WRITER_SCHEMA);
//      // No output stream
//    }

      @UseDataSet("clusterTable")
      private ClusterTable clusterTable;

      @Override
      public FlowletSpecification configure() {
          return FlowletSpecification.Builder.with()
                  .setName(ClusterWriterFlow.writer_output)
                  .setDescription("Cluster writer flowlet")
                  .useDataSet("clusterTable")
                  .build();
      }



    @Override
    public void initialize(FlowletContext context) throws FlowletException {
      this.clusterTable = new ClusterTable(LishApp.CLUSTER_TABLE);

       // deprecated?
 //     getFlowletContext().getDataSetRegistry().registerDataSet(
 //         this.clusterTable);
    }

      @ProcessInput
      public void process(CLUSTER_PARSER_TO_WRITER_SCHEMA writeSchema) {

          try {
              this.clusterTable.writeCluster(writeSchema.clusterId, writeSchema.category, writeSchema.weight);

              // TODO: add logging
              // getFlowletContext().getLogger().debug("Writing cluster (id=" + writeSchema.clusterId + ",
              // category=" + writeSchema.category + ", weight=" +
          } catch (Exception e) {

          } finally {
              numProcessed++;
          }
      }
  }

  /**
   * Flowlet that clears existing clusters from the data fabric.
   */
  public class ClusterReset extends AbstractFlowlet {

     int numProcessed = 0;

//    @Override
//    public void configure(StreamsConfigurator configurator) {
//      // Apply cluster tuple schema to default tuple input stream
//      configurator
//          .getDefaultTupleInputStream()
//          .setSchema(CLUSTER_PARSER_TO_RESET_SCHEMA);
//      // No output stream
//    }

     @Override

     public FlowletSpecification configure() {
         return FlowletSpecification.Builder.with()
                 .setName(ClusterWriterFlow.reset_output)
                 .setDescription("Cluster reset flowlet")
                 .useDataSet("clustertable")
                 .build();
     }

    @UseDataSet("clusterTable")
    private ClusterTable clusterTable;

    @Override
    public void initialize(FlowletContext context) throws FlowletException {
      this.clusterTable = new ClusterTable(LishApp.CLUSTER_TABLE);


//      getFlowletContext().getDataSetRegistry().registerDataSet(
//          this.clusterTable);
    }

//    @Override
//    public void process(Tuple tuple, TupleContext context,
//        OutputCollector collector) {
//      Integer maxClusterId = tuple.get("maxClusterId");
//      try {
//        this.clusterTable.resetClusters(maxClusterId);
//      } catch (OperationException e) {
//        getFlowletContext().getLogger().error("Error resetting clusters", e);
//        return;
//      }
//      getFlowletContext().getLogger().info(
//          "Resetting clusters using maxId " + maxClusterId);
//    }

    @ProcessInput
    public void process(CLUSTER_PARSER_TO_RESET_SCHEMA resetSchema) {
        try {
            this.clusterTable.resetClusters(resetSchema.maxClusterId);
            this.numProcessed++;

        } catch (OperationException e) {

            // TODO: Add logging
            // getFlowletContext().getLogger().error("Error resetting clusters", e);
            return;
        }
    }

    // Deprecated
//    @Override
//    public void onSuccess(Tuple tuple, TupleContext context) {
//      numProcessed++;
//    }

//    @Override
//    public FailureHandlingPolicy onFailure(Tuple tuple, TupleContext context,
//        FailureReason reason) {
//      getFlowletContext().getLogger().error(
//          "ClusterReset Flowet Processing Failed : " + reason.toString() +
//          ", retrying");
//      return FailureHandlingPolicy.RETRY;
//    }

  }
}
