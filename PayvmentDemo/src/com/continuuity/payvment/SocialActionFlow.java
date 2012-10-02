package com.continuuity.payvment;


import com.continuuity.api.data.Increment;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.Write;
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
import com.continuuity.payvment.data.ActivityFeed;
import com.continuuity.payvment.data.ActivityFeed.ActivityFeedEntry;
import com.continuuity.payvment.data.CounterTable;
import com.continuuity.payvment.data.ProductTable;
import com.continuuity.payvment.data.SortedCounterTable;
import com.continuuity.payvment.util.Bytes;
import com.continuuity.payvment.util.Constants;
import com.continuuity.payvment.util.Helpers;

public class SocialActionFlow implements Flow {

  static final String inputStream = "social-actions";

  static final String flowName = "SocialActionProcessor";

  @Override
  public void configure(FlowSpecifier specifier) {

    // Set metadata fields
    specifier.name(flowName);
    specifier.email("dev@continuuity.com");
    specifier.application("Cluster Activity Feeds");
    specifier.company("Continuuity+Payvment");

    // Declare all of the flowlets within the flow
    specifier.flowlet("action_parser", SocialActionParserFlowlet.class,
        1);
    specifier.flowlet("action_processor", SocialActionProcessorFlowlet.class,
        1);
    specifier.flowlet("activity_feed_updater", ActivityFeedUpdaterFlowlet.class,
        1);
    specifier.flowlet("popular_feed_updater", PopularFeedUpdaterFlowlet.class,
        1);

    // Define user_follow_events stream and connect to json_source_parser
    specifier.stream(inputStream);
    specifier.input(inputStream, "action_parser");

    // Wire up the remaining flowlet connections
    specifier.connection("action_parser", "action_processor");
    specifier.connection("action_processor", "activity_feed_updater");
    specifier.connection("action_processor", "popular_feed_updater");
  }

  public static final TupleSchema SOCIAL_ACTION_TUPLE_SCHEMA =
      new TupleSchemaBuilder().add("action", SocialAction.class).create();

  public static final TupleSchema PROCESSED_ACTION_TUPLE_SCHEMA =
      new TupleSchemaBuilder()
          .add("action", SocialAction.class)
          .add("product-meta", ProductMeta.class)
          .add("category", String.class)
          .add("score-increase", Long.class)
          .add("all-time-score", Long.class)
          .add("hourly-score", Long.class)
          .create();

  public static class SocialActionProcessorFlowlet extends ComputeFlowlet {

    static int numProcessed = 0;

    static int numErrors = 0;

    @Override
    public void configure(StreamsConfigurator configurator) {
      // Input schema is a social action
      configurator.getDefaultTupleInputStream()
          .setSchema(SOCIAL_ACTION_TUPLE_SCHEMA);
      // Output schema contains action, category, score, Increment pass-thrus
      configurator.getDefaultTupleOutputStream()
          .setSchema(PROCESSED_ACTION_TUPLE_SCHEMA);
    }

    private ProductTable productTable;

    private CounterTable productActionCountTable;

    private CounterTable allTimeScoreTable;

    private SortedCounterTable topScoreTable;

    @Override
    public void initialize() {
      this.productTable = new ProductTable(getFlowletContext());
      this.productActionCountTable = new CounterTable("productActions",
          getFlowletContext());
      this.allTimeScoreTable = new CounterTable("allTimeScore",
          getFlowletContext());
      this.topScoreTable = new SortedCounterTable("topScore",
          getFlowletContext(), new SortedCounterTable.SortedCounterConfig());
    }

    @Override
    public void process(Tuple tuple, TupleContext context,
        OutputCollector collector) {
      SocialAction action = tuple.get("action");
      TupleBuilder tupleBuilder = new TupleBuilder();
      tupleBuilder.set("action", action);
      
      // Read the category from product table
      ProductMeta productMeta;
      try {
        productMeta = productTable.readObject(
            Bytes.toBytes(action.product_id));
      } catch (OperationException e) {
        e.printStackTrace();
        throw new RuntimeException("Error reading product metadata");
      }
      if (productMeta == null) {
        // Received social action for unknown item, don't know what to do yet
        String msg = "Received social action for unknown product (" +
            action.product_id + ")";
        System.out.println(msg);
        numErrors++;
        throw new RuntimeException(msg);
      }
      tupleBuilder.set("product-meta", productMeta);
      
      // Update product action count table async
      this.productActionCountTable.incrementCounterSet(
          Bytes.toBytes(action.product_id), Bytes.toBytes(action.type), 1L);
      
      // Determine score increase
      Long scoreIncrease = action.getSocialActionType().getScore();
      tupleBuilder.set("score-increase", scoreIncrease);
      
      // Update all-time score, but put increment into tuple for pass-thru
      Increment allTimeScore = allTimeScoreTable.generateSingleKeyIncrement(
          Bytes.add(Constants.PRODUCT_ALL_TIME_PREFIX,
              Bytes.toBytes(action.product_id)), scoreIncrease);
      tupleBuilder.set("all-time-score", allTimeScore);
      
      // Update time bucketed top-score table, also put increment into tuple
      Increment topScoreHourly = topScoreTable.generatePrimaryCounterIncrement(
          Bytes.add(Bytes.toBytes(Helpers.hour(action.date)),
              Bytes.toBytes(productMeta.category)),
          Bytes.toBytes(action.product_id), scoreIncrease);
      tupleBuilder.set("hourly-score", topScoreHourly);
      
      collector.add(tupleBuilder.create());
    }

    @Override
    public void onSuccess(Tuple tuple, TupleContext context) {
      numProcessed++;
    }
  }

  public static class ActivityFeedUpdaterFlowlet extends ComputeFlowlet {

    static int numProcessed = 0;

    @Override
    public void configure(StreamsConfigurator configurator) {
      // Input schema contains action, category, and Increment pass-thrus
      configurator.getDefaultTupleInputStream()
          .setSchema(PROCESSED_ACTION_TUPLE_SCHEMA);
      // No output
    }

    @Override
    public void process(Tuple tuple, TupleContext context,
        OutputCollector collector) {
      Long scoreIncrease = tuple.get("score-increase");
      Long allTimeScore = tuple.get("all-time-score");
      Long hourlyScore = tuple.get("hourly-score");
      if (!shouldInsertFeedEntry(scoreIncrease, allTimeScore, hourlyScore)) {
        return;
      }
      // Insert feed entry
      SocialAction action = tuple.get("action");
      ProductMeta meta = tuple.get("product-meta");
      ActivityFeedEntry feedEntry = new ActivityFeedEntry(action.date,
          meta.store_id, meta.product_id, allTimeScore);
      Write feedEntryWrite = new Write(Constants.ACTIVITY_FEED_TABLE,
          ActivityFeed.makeActivityFeedRow(meta.category),
          feedEntry.getColumn(), feedEntry.getValue());
      collector.add(feedEntryWrite);
    }
    
    private static boolean shouldInsertFeedEntry(Long scoreIncrease,
        Long allTimeScore, Long hourlyScore) {
      return true;
    }

    @Override
    public void onSuccess(Tuple tuple, TupleContext context) {
      numProcessed++;
    }
  }

  public static class PopularFeedUpdaterFlowlet extends ComputeFlowlet {

    static int numProcessed = 0;

    @Override
    public void configure(StreamsConfigurator configurator) {
      // Input schema contains action, category, and Increment pass-thrus
      configurator.getDefaultTupleInputStream()
          .setSchema(PROCESSED_ACTION_TUPLE_SCHEMA);
      // No output
    }

    private SortedCounterTable topScoreTable;

    @Override
    public void initialize() {
      this.topScoreTable = new SortedCounterTable("topScore",
          getFlowletContext(), new SortedCounterTable.SortedCounterConfig());
    }

    @Override
    public void process(Tuple tuple, TupleContext context,
        OutputCollector collector) {
      Long scoreIncrease = tuple.get("score-increase");
      Long hourlyScore = tuple.get("hourly-score");
      SocialAction action = tuple.get("action");
      ProductMeta productMeta = tuple.get("product-meta");
      // Let top score perform any additional indexing increments
      this.topScoreTable.performSecondaryCounterIncrements(
          Bytes.add(Bytes.toBytes(Helpers.hour(action.date)),
              Bytes.toBytes(productMeta.category)),
          Bytes.toBytes(action.product_id), scoreIncrease, hourlyScore);
    }

    @Override
    public void onSuccess(Tuple tuple, TupleContext context) {
      numProcessed++;
    }
  }
}
