package com.payvment.continuuity;

import com.continuuity.api.data.Increment;
import com.continuuity.api.data.lib.CounterTable;
import com.continuuity.api.data.lib.SortedCounterTable;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.util.Helpers;
import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecifier;
import com.continuuity.api.flow.flowlet.ComputeFlowlet;
import com.continuuity.api.flow.flowlet.FlowletSpecifier;
import com.continuuity.api.flow.flowlet.OutputCollector;
import com.continuuity.api.flow.flowlet.Tuple;
import com.continuuity.api.flow.flowlet.TupleContext;
import com.continuuity.api.flow.flowlet.TupleSchema;
import com.continuuity.api.flow.flowlet.builders.TupleBuilder;
import com.continuuity.api.flow.flowlet.builders.TupleSchemaBuilder;
import com.payvment.continuuity.data.ActivityFeed.ActivityFeedEntry;
import com.payvment.continuuity.data.ActivityFeedTable;
import com.payvment.continuuity.data.ProductTable;
import com.payvment.continuuity.entity.ProductFeedEntry;

public class ProductFeedFlow implements Flow {

  static final String inputStream = "product-feed";

  static final String flowName = "ProductFeedProcessor";
  
  @Override
  public void configure(FlowSpecifier specifier) {

    // Set metadata fields
    specifier.name(flowName);
    specifier.email("dev@continuuity.com");
    specifier.application("ClusterFeeds");
    specifier.company("Continuuity+Payvment");

    // Declare all of the flowlets within the flow
    specifier.flowlet("product_parser", ProductFeedParserFlowlet.class, 1);
    specifier.flowlet("product_processor", ProductProcessorFlowlet.class, 1);
    specifier.flowlet("activity_feed_updater",
        ProductActivityFeedUpdaterFlowlet.class, 1);

    // Define the input stream and connect to parser
    specifier.stream(inputStream);
    specifier.input(inputStream, "product_parser");

    // Wire up the remaining flowlet connections
    specifier.connection("product_parser", "product_processor");
    specifier.connection("product_processor", "activity_feed_updater");
  }

  public static final TupleSchema PRODUCT_META_TUPLE_SCHEMA =
      new TupleSchemaBuilder().add("product", ProductFeedEntry.class).create();

  public static final TupleSchema PROCESSED_PRODUCT_TUPLE_SCHEMA =
      new TupleSchemaBuilder()
          .add("product", ProductFeedEntry.class)
          .add("update-count", Long.class)
          .add("all-time-score", Long.class)
          .add("hourly-score", Long.class)
          .create();


  public static class ProductProcessorFlowlet extends ComputeFlowlet {

    static int numProcessed = 0;

    @Override
    public void configure(FlowletSpecifier configurator) {
      // Input schema is product meta
      configurator.getDefaultFlowletInput()
          .setSchema(PRODUCT_META_TUPLE_SCHEMA);
      // Output schema is product meta and update count
      configurator.getDefaultFlowletOutput()
          .setSchema(PROCESSED_PRODUCT_TUPLE_SCHEMA);
    }

    private ProductTable productTable;

    private CounterTable productUpdateCountTable;

    private CounterTable allTimeScoreTable;

    private SortedCounterTable topScoreTable;

    @Override
    public void initialize() {
      this.productTable = new ProductTable();
      getFlowletContext().getDataSetRegistry().registerDataSet(
          this.productTable);
      this.productUpdateCountTable = new CounterTable("productUpdates");
      getFlowletContext().getDataSetRegistry().registerDataSet(
          this.productUpdateCountTable);
      this.allTimeScoreTable = new CounterTable("allTimeScores");
      getFlowletContext().getDataSetRegistry().registerDataSet(
          this.allTimeScoreTable);
      this.topScoreTable = new SortedCounterTable("topScores",
          new SortedCounterTable.SortedCounterConfig());
      getFlowletContext().getDataSetRegistry().registerDataSet(
          this.topScoreTable);
    }

    @Override
    public void process(Tuple tuple, TupleContext context,
        OutputCollector collector) {
      ProductFeedEntry productMeta = tuple.get("product");
      
      // Write product meta data
      productTable.updateObject(Bytes.toBytes(productMeta.product_id),
          productMeta);
      TupleBuilder tupleBuilder = new TupleBuilder();
      tupleBuilder.set("product", productMeta);
      
      // Count number of product updates and pass-thru
      Increment updateCountInc =
          this.productUpdateCountTable.generateSingleKeyIncrement(
              Bytes.toBytes(productMeta.product_id), 1L);
      tupleBuilder.set("update-count", updateCountInc);
      
      // Add one to item score and pass-thru
      Increment itemScoreInc =
          this.allTimeScoreTable.generateSingleKeyIncrement(
                  Bytes.toBytes(productMeta.product_id), 1L);
      tupleBuilder.set("all-time-score", itemScoreInc);
      
      // Update time bucketed top-score table, also put increment into tuple
      Increment topScoreHourly = topScoreTable.generatePrimaryCounterIncrement(
          Bytes.add(Bytes.toBytes(Helpers.hour(productMeta.date)),
              Bytes.toBytes(productMeta.category)),
          Bytes.toBytes(productMeta.product_id), 1L);
      tupleBuilder.set("hourly-score", topScoreHourly);
      collector.add(tupleBuilder.create());
    }

    @Override
    public void onSuccess(Tuple tuple, TupleContext context) {
      numProcessed++;
    }
  }
  
  public static class ProductActivityFeedUpdaterFlowlet extends ComputeFlowlet {

    static int numProcessed = 0;

    @Override
    public void configure(FlowletSpecifier configurator) {
      // Input schema is product meta and update count
      configurator.getDefaultFlowletInput()
          .setSchema(PROCESSED_PRODUCT_TUPLE_SCHEMA);
      // No output
    }

    private SortedCounterTable topScoreTable;

    private ActivityFeedTable activityFeedTable;
    
    @Override
    public void initialize() {
      this.topScoreTable = new SortedCounterTable("topScores",
          new SortedCounterTable.SortedCounterConfig());
      getFlowletContext().getDataSetRegistry().registerDataSet(
          this.topScoreTable);
      this.activityFeedTable = new ActivityFeedTable();
      getFlowletContext().getDataSetRegistry().registerDataSet(
          this.activityFeedTable);
    }

    @Override
    public void process(Tuple tuple, TupleContext context,
        OutputCollector collector) {
      ProductFeedEntry productMeta = tuple.get("product");
      Long updateCount = tuple.get("update-count");
      Long allTimeScore = tuple.get("all-time-score");
      Long hourlyScore = tuple.get("hourly-score");
      if (!shouldInsertFeedEntry(productMeta, updateCount)) {
        return;
      }
      // Insert feed entry
      ActivityFeedEntry feedEntry = new ActivityFeedEntry(productMeta.date,
          productMeta.store_id, productMeta.product_id, allTimeScore);
      for (String country : productMeta.country) {
        activityFeedTable.writeEntry(country, productMeta.category, feedEntry);
      }

      // Let top score perform any additional indexing increments
      this.topScoreTable.performSecondaryCounterIncrements(
          Bytes.add(Bytes.toBytes(Helpers.hour(productMeta.date)),
              Bytes.toBytes(productMeta.category)),
          Bytes.toBytes(productMeta.product_id), 1L, hourlyScore);
    }

    @Override
    public void onSuccess(Tuple tuple, TupleContext context) {
      numProcessed++;
    }
    
    private static boolean shouldInsertFeedEntry(ProductFeedEntry productMeta,
        Long updateCount) {
      return true;
    }
  }
}
