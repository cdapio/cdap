package com.continuuity.payvment;

import java.io.IOException;

import au.com.bytecode.opencsv.CSVParser;

import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecifier;
import com.continuuity.api.flow.flowlet.ComputeFlowlet;
import com.continuuity.api.flow.flowlet.OutputCollector;
import com.continuuity.api.flow.flowlet.StreamsConfigurator;
import com.continuuity.api.flow.flowlet.Tuple;
import com.continuuity.api.flow.flowlet.TupleContext;
import com.continuuity.api.flow.flowlet.TupleSchema;
import com.continuuity.api.flow.flowlet.builders.TupleBuilder;
import com.continuuity.api.flow.flowlet.builders.TupleSchemaBuilder;
import com.continuuity.payvment.data.ClusterTable;
import com.continuuity.payvment.util.Constants;

public class ClusterWriterFlow implements Flow {

  @Override
  public void configure(FlowSpecifier specifier) {

    // Set metadata fields
    specifier.name("Cluster Writer");
    specifier.email("dev@continuuity.com");
    specifier.application("Cluster Activity Feeds");
    specifier.company("Continuuity+Payvment");

    // Declare all of the flowlets within the flow
    specifier.flowlet("cluster_source_parser", ClusterSourceParser.class, 1);
    specifier.flowlet("cluster_writer", ClusterWriter.class, 1);
    specifier.flowlet("cluster_reset", ClusterReset.class, 1);

    // Define user_follow_events stream and connect to json_source_parser
    specifier.stream("clusters");
    specifier.input("clusters", "cluster_source_parser");

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

    @Override
    public void configure(StreamsConfigurator configurator) {

      // Apply default stream schema to default tuple input stream
      configurator
          .getDefaultTupleInputStream()
          .setSchema(TupleSchema.EVENT_SCHEMA);

      // Apply internal cluster tuple schema to writer output stream
      configurator
          .addTupleOutputStream("writer_output")
          .setSchema(CLUSTER_PARSER_TO_WRITER_SCHEMA);
      
      // Apply simple reset tuple schema to reset output stream
      configurator
          .addTupleOutputStream("reset_output")
          .setSchema(CLUSTER_PARSER_TO_RESET_SCHEMA);
      
    }

    private final CSVParser parser = new CSVParser(',', '"', '\\', false);

    @Override
    public void process(Tuple tuple, TupleContext context,
        OutputCollector collector) {

      // Grab CSV string from event-stream tuple
      String csvEventString = new String((byte[])tuple.get("body"));
      
      // Parse as CSV
      String [] parsed = null;
      try {
        parsed = parser.parseLine(csvEventString);
        if (parsed.length != 3) throw new IOException();
      } catch (IOException e) {
        throw new RuntimeException("Invalid input string: " + csvEventString);
      }
      
      // Check if special flag to reset clusters exists
      if (parsed[0].equals(Constants.CLUSTER_RESET_FLAG)) {
        // CSV = reset_clusters,max_cluster_id,"msg"
        Tuple resetTuple = new TupleBuilder()
            .set("maxClusterId", parsed[1])
            .set("msg", parsed[2])
            .create();
        collector.add("reset_output", resetTuple);
        return;
      }
      
      // Format of CSV string is: clusterid,category,weight
      Tuple clusterTuple = new TupleBuilder()
          .set("clusterId", Integer.valueOf(parsed[0]))
          .set("category", parsed[1])
          .set("weight", Double.valueOf(parsed[2]))
          .create();
      collector.add(clusterTuple);
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
  
  public static class ClusterWriter extends ComputeFlowlet {

    @Override
    public void configure(StreamsConfigurator configurator) {
      // Apply cluster tuple schema to default tuple input stream
      configurator
          .getDefaultTupleInputStream()
          .setSchema(CLUSTER_PARSER_TO_WRITER_SCHEMA);
      // No output stream
    }

    private ClusterTable clusterTable;

    @Override
    public void initialize() {
      this.clusterTable = new ClusterTable(getFlowletContext());
    }
    
    @Override
    public void process(Tuple tuple, TupleContext context,
        OutputCollector collector) {
      Integer clusterId = tuple.get("clusterId");
      String category = tuple.get("category");
      Double weight = tuple.get("weight");
      this.clusterTable.writeCluster(clusterId, category, weight);
    }

  }
  
  public static class ClusterReset extends ComputeFlowlet {

    @Override
    public void configure(StreamsConfigurator configurator) {
      // Apply cluster tuple schema to default tuple input stream
      configurator
          .getDefaultTupleInputStream()
          .setSchema(CLUSTER_PARSER_TO_RESET_SCHEMA);
      // No output stream
    }

    private ClusterTable clusterTable;

    @Override
    public void initialize() {
      this.clusterTable = new ClusterTable(getFlowletContext());
    }
    
    @Override
    public void process(Tuple tuple, TupleContext context,
        OutputCollector collector) {
      int maxClusterId = tuple.get("maxClusterId");
      String msg = tuple.get("msg");
      System.out.println("Cluster Reset.  Message: " + msg);
      this.clusterTable.resetClusters(maxClusterId);
    }

  }
}
