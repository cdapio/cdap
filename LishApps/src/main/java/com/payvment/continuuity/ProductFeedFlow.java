//package com.payvment.continuuity;
//
//import com.continuuity.api.annotation.Output;
//import com.continuuity.api.annotation.ProcessInput;
//import com.continuuity.api.annotation.UseDataSet;
//import com.continuuity.api.common.Bytes;
//import com.continuuity.api.data.OperationException;
//import com.continuuity.api.data.dataset.table.Increment;
//import com.continuuity.api.data.util.Helpers;
//import com.continuuity.api.flow.Flow;
//import com.continuuity.api.flow.FlowSpecification;
//import com.continuuity.api.flow.flowlet.AbstractFlowlet;
//import com.continuuity.api.flow.flowlet.FlowletContext;
//import com.continuuity.api.flow.flowlet.FlowletSpecification;
//import com.continuuity.api.flow.flowlet.OutputEmitter;
//import com.google.common.base.Throwables;
//import com.payvment.continuuity.data.ActivityFeed.ActivityFeedEntry;
//import com.payvment.continuuity.data.ActivityFeedTable;
//import com.payvment.continuuity.data.CounterTable;
//import com.payvment.continuuity.data.ProductTable;
//import com.payvment.continuuity.data.SortedCounterTable;
//import com.payvment.continuuity.entity.ProductFeedEntry;
//
//public class ProductFeedFlow implements Flow {
//
//    public static final String inputStream = "product-feed";
//
//    static final String flowName = "ProductFeedProcessor";
//    static final String flow_description = "ProductFeedProcessor";
//
//    @Override
//    public FlowSpecification configure() {
//        return FlowSpecification.Builder.with()
//                .setName(flowName)
//                .setDescription(flow_description)
//                .withFlowlets()
//                .add("product_parser", new ProductFeedParserFlowlet())
//                .add("product_processor", new ProductProcessorFlowlet())
//                .add("activity_feed_updater", new ProductActivityFeedUpdaterFlowlet())
//                .connect()
//                .fromStream(inputStream).to("product_parser")
//                .from("product_parser").to("product_processor")
//                .from("product_processor").to("activity_feed_updater")
//                .build();
//    }
//
//// Old logic
////    // Set metadata fields
////    specifier.name(flowName);
////    specifier.email("dev@continuuity.com");
////    specifier.application("ClusterFeeds");
////    specifier.company("Continuuity+Payvment");
////
////    // Declare all of the flowlets within the flow
////    specifier.flowlet("product_parser", ProductFeedParserFlowlet.class, 1);
////    specifier.flowlet("product_processor", ProductProcessorFlowlet.class, 1);
////    specifier.flowlet("activity_feed_updater",
////        ProductActivityFeedUpdaterFlowlet.class, 1);
////
////    // Define the input stream and connect to parser
////    specifier.stream(inputStream);
////    specifier.input(inputStream, "product_parser");
////
////    // Wire up the remaining flowlet connections
////    specifier.connection("product_parser", "product_processor");
////    specifier.connection("product_processor", "activity_feed_updater");
//
//
//    //   public class PRODUCT_META_TUPLE_SCHEMA {
//    //     public ProductFeedEntry product;
//    //  }
////  public static final TupleSchema PRODUCT_META_TUPLE_SCHEMA =
////      new TupleSchemaBuilder().add("product", ProductFeedEntry.class).create();
//
//
//    public class ProcessedProductStats {
//        public ProductFeedEntry product;
//        public Long update_count;
//        public Long all_time_score;
//        public Long hourly_score;
//    }
//
//// Old logic
////  public static final TupleSchema PROCESSED_PRODUCT_TUPLE_SCHEMA =
////      new TupleSchemaBuilder()
//
////          .add("product", ProductFeedEntry.class)
////          .add("update-count", Long.class)
////          .add("all-time-score", Long.class)
////          .add("hourly-score", Long.class)
////          .create();
//
//
//    public class ProductProcessorFlowlet extends AbstractFlowlet {
//
//        int numProcessed = 0;
//
//        @Override
//        public FlowletSpecification configure() {
//            return FlowletSpecification.Builder.with()
//                    .setName("product_processor")
//                    .setDescription("product processor reset flowlet")
//                    .useDataSet("productTable")
//                    .useDataSet("productUpdates")
//                    .useDataSet("allTimeScores")
//                    .build();
//        }
//
//        @UseDataSet("productTable")
//        private ProductTable productTable;
//
//        @UseDataSet("productUpdateCountTable")
//        private CounterTable productUpdateCountTable;
//
//        @UseDataSet("allTimeScoreTable")
//        private CounterTable allTimeScoreTable;
//
//        @UseDataSet("topScoreTable")
//        private SortedCounterTable topScoreTable;
//
//        @Override
//        public void initialize(FlowletContext context) {
//
//            this.productTable = new ProductTable("productTable");
//            this.productUpdateCountTable = new CounterTable("productUpdates");
//            this.allTimeScoreTable = new CounterTable("allTimeScores");
//            this.topScoreTable = new SortedCounterTable("topScores", new SortedCounterTable.SortedCounterConfig());
//        }
//
//        @Output("processed_product")
//        OutputEmitter<ProcessedProductStats> processProductOutputEmitter;
//
//
//        @ProcessInput
//        public void process(ProcessedProductStats prodStats) {
//
//            // Write product metadata
//            ProductFeedEntry productMeta = prodStats.product;
//            productTable.updateObject(productMeta.product_id, productMeta);
//
//            // Count number of product updates and pass-thru
//            Increment updateCountInc =
//                    this.productUpdateCountTable.generateSingleKeyIncrement(
//                            Bytes.toBytes(productMeta.product_id), 1L);
//
//            // TODO: Make sure reading first value from increment is legit.
//            prodStats.update_count = updateCountInc.getValues()[0];
//
//            // Add one to item score and pass-thru
//            Increment itemScoreInc =
//                    this.allTimeScoreTable.generateSingleKeyIncrement(
//                            Bytes.toBytes(productMeta.product_id), 1L);
//
//            prodStats.all_time_score = itemScoreInc.getValues()[0];
//
//
//            // Update time bucketed top-score table, also put increment into tuple
//            try {
//
//                topScoreTable.increment(
//                        Bytes.add(Bytes.toBytes(Helpers.hour(productMeta.date)),
//                                Bytes.toBytes(productMeta.category)),
//                        Bytes.toBytes(productMeta.product_id), 1L);
//            } catch (OperationException e) {
//                Throwables.propagate(e);
//            }
//
//            //TODO: read topScoreHourly back from the table OR read from ProductFeedFlowlet
//            //prodStats.hourly_score = topScoreHourly.getValues()[0];
//            //tupleBuilder.set("hourly-score", topScoreHourly);
//
//            processProductOutputEmitter.emit(prodStats);
//        }
//
//
//// Old logic
////    @Override
////    public void process(Tuple tuple, TupleContext context,
////        OutputCollector collector) {
////      ProductFeedEntry productMeta = tuple.get("product");
////
////      // Write product meta data
////      productTable.updateObject(Bytes.toBytes(productMeta.product_id),
////          productMeta);
////      TupleBuilder tupleBuilder = new TupleBuilder();
////      tupleBuilder.set("product", productMeta);
////
////      // Count number of product updates and pass-thru
////      Increment updateCountInc =
////          this.productUpdateCountTable.generateSingleKeyIncrement(
////              Bytes.toBytes(productMeta.product_id), 1L);
////      tupleBuilder.set("update-count", updateCountInc);
////
////      // Add one to item score and pass-thru
////      Increment itemScoreInc =
////          this.allTimeScoreTable.generateSingleKeyIncrement(
////                  Bytes.toBytes(productMeta.product_id), 1L);
////      tupleBuilder.set("all-time-score", itemScoreInc);
////
////      // Update time bucketed top-score table, also put increment into tuple
////      Increment topScoreHourly = topScoreTable.generatePrimaryCounterIncrement(
////          Bytes.add(Bytes.toBytes(Helpers.hour(productMeta.date)),
////              Bytes.toBytes(productMeta.category)),
////          Bytes.toBytes(productMeta.product_id), 1L);
////      tupleBuilder.set("hourly-score", topScoreHourly);
////      collector.add(tupleBuilder.create());
////    }
//
//
////    @Override
////    public void onSuccess(Tuple tuple, TupleContext context) {
////      numProcessed++;
////    }
//    }
//
//    public static class ProductActivityFeedUpdaterFlowlet extends AbstractFlowlet {
//
//        static int numProcessed = 0;
//
////    @Override
////    public void configure(StreamsConfigurator configurator) {
////      // Input schema is product meta and update count
////      configurator.getDefaultTupleInputStream()
////          .setSchema(PROCESSED_PRODUCT_TUPLE_SCHEMA);
////      // No output
////    }
//
//        @Override
//        public FlowletSpecification configure() {
//            return FlowletSpecification.Builder.with()
//                    .setName("activity_feed_updater")
//                    .setDescription("Product Activity FeedUpdater Flowlet")
//                    .build();
//        }
//
//
//        private SortedCounterTable topScoreTable;
//
//        private ActivityFeedTable activityFeedTable;
//
//        @Override
//        public void initialize(FlowletContext context) {
//            this.topScoreTable = new SortedCounterTable(LishApp.TOP_SCORE_TABLE,
//                    new SortedCounterTable.SortedCounterConfig());
//
//            this.activityFeedTable = new ActivityFeedTable(LishApp.ACTIVITY_FEED_TABLE);
//        }
//
//        @ProcessInput
//        public void process(ProcessedProductStats productStats) {
//
//            // Insert feed entry
//            ActivityFeedEntry feedEntry = new ActivityFeedEntry(productStats.product.date,
//                    productStats.product.store_id, productStats.product.product_id, productStats.all_time_score);
//
//            for (String country : productStats.product.country) {
//                activityFeedTable.writeEntry(country, productStats.product.category, feedEntry);
//            }
//
//            // TODO: fix additional indexing increment
//            // Let top score perform any additional indexing increments
//            try {
//            this.topScoreTable.increment(
//                    Bytes.add(Bytes.toBytes(Helpers.hour(productStats.product.date)),
//                            Bytes.toBytes(productStats.product.category)),
//                    Bytes.toBytes(productStats.product.product_id), /*1L*/ productStats.hourly_score);
//
//            numProcessed++;
//            }
//            catch (OperationException e) {
//                Throwables.propagate(e);
//            }
//        }
//
//// Old logic
////    @Override
////    public void process(Tuple tuple, TupleContext context,
////        OutputCollector collector) {
////      ProductFeedEntry productMeta = tuple.get("product");
////      Long updateCount = tuple.get("update-count");
////      Long allTimeScore = tuple.get("all-time-score");
////      Long hourlyScore = tuple.get("hourly-score");
////      if (!shouldInsertFeedEntry(productMeta, updateCount)) {
////        return;
////      }
////      // Insert feed entry
////      ActivityFeedEntry feedEntry = new ActivityFeedEntry(productMeta.date,
////          productMeta.store_id, productMeta.product_id, allTimeScore);
////      for (String country : productMeta.country) {
////        activityFeedTable.writeEntry(country, productMeta.category, feedEntry);
////      }
////
////      // Let top score perform any additional indexing increments
////      this.topScoreTable.performSecondaryCounterIncrements(
////          Bytes.add(Bytes.toBytes(Helpers.hour(productMeta.date)),
////              Bytes.toBytes(productMeta.category)),
////          Bytes.toBytes(productMeta.product_id), 1L, hourlyScore);
////    }
//
////    @Override
////    public void onSuccess(Tuple tuple, TupleContext context) {
////      numProcessed++;
////    }
//    }
//}
