package com.continuuity.payvment;

import com.continuuity.api.data.Increment;
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
import com.continuuity.payvment.util.Bytes;
import com.continuuity.payvment.util.Constants;

public class ProductFeedFlow implements Flow {

  static final String inputStream = "product-feed";

  static final String flowName = "ProductFeedProcessor";
  
  @Override
  public void configure(FlowSpecifier specifier) {

    // Set metadata fields
    specifier.name(flowName);
    specifier.email("dev@continuuity.com");
    specifier.application("Cluster Activity Feeds");
    specifier.company("Continuuity+Payvment");

    // Declare all of the flowlets within the flow
    specifier.flowlet("product_parser", ProductFeedParserFlowlet.class, 1);
    specifier.flowlet("product_processor", ProductProcessorFlowlet.class, 1);
    specifier.flowlet("activity_feed_updater",
        ProductActivityFeedUpdaterFlowlet.class, 1);

    // Define user_follow_events stream and connect to json_source_parser
    specifier.stream(inputStream);
    specifier.input(inputStream, "product_parser");

    // Wire up the remaining flowlet connections
    specifier.connection("product_parser", "product_processor");
    specifier.connection("product_processor", "activity_feed_updater");
  }

  public static final TupleSchema PRODUCT_META_TUPLE_SCHEMA =
      new TupleSchemaBuilder().add("product", ProductMeta.class).create();

  public static final TupleSchema PROCESSED_PRODUCT_TUPLE_SCHEMA =
      new TupleSchemaBuilder()
          .add("product", ProductMeta.class)
          .add("update-count", Long.class)
          .add("all-time-score", Long.class)
          .create();


  public static class ProductProcessorFlowlet extends ComputeFlowlet {

    static int numProcessed = 0;

    @Override
    public void configure(StreamsConfigurator configurator) {
      // Input schema is product meta
      configurator.getDefaultTupleInputStream()
          .setSchema(PRODUCT_META_TUPLE_SCHEMA);
      // Output schema is product meta and update count
      configurator.getDefaultTupleOutputStream()
          .setSchema(PROCESSED_PRODUCT_TUPLE_SCHEMA);
    }

    private ProductTable productTable;

    private CounterTable productUpdateCountTable;

    private CounterTable allTimeScoreTable;

    @Override
    public void initialize() {
      this.productTable = new ProductTable(getFlowletContext());
      this.productUpdateCountTable = new CounterTable("productUpdates",
          getFlowletContext());
      this.allTimeScoreTable = new CounterTable("allTimeScore",
          getFlowletContext());
    }

    @Override
    public void process(Tuple tuple, TupleContext context,
        OutputCollector collector) {
      ProductMeta productMeta = tuple.get("product");
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
              Bytes.add(Constants.PRODUCT_ALL_TIME_PREFIX,
                  Bytes.toBytes(productMeta.product_id)), 1L);
      tupleBuilder.set("all-time-score", itemScoreInc);
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
    public void configure(StreamsConfigurator configurator) {
      // Input schema is product meta and update count
      configurator.getDefaultTupleInputStream()
          .setSchema(PROCESSED_PRODUCT_TUPLE_SCHEMA);
      // No output
    }

    @Override
    public void process(Tuple tuple, TupleContext context,
        OutputCollector collector) {
      ProductMeta productMeta = tuple.get("product");
      long updateCount = tuple.get("update-count");
      long allTimeScore = tuple.get("all-time-score");
      if (!shouldInsertFeedEntry(productMeta, updateCount)) {
        return;
      }
      // Insert feed entry
      ActivityFeedEntry feedEntry = new ActivityFeedEntry(productMeta.date,
          productMeta.store_id, productMeta.product_id, allTimeScore);
      Write feedEntryWrite = new Write(Constants.ACTIVITY_FEED_TABLE,
          ActivityFeed.makeActivityFeedRow(productMeta.category),
          feedEntry.getColumn(), feedEntry.getValue());
      collector.add(feedEntryWrite);
    }

    @Override
    public void onSuccess(Tuple tuple, TupleContext context) {
      numProcessed++;
    }
    
    private static boolean shouldInsertFeedEntry(ProductMeta productMeta,
        Long updateCount) {
      return true;
    }
  }
}
