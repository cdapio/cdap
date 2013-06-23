//package com.payvment.continuuity;
//
//import com.continuuity.api.annotation.ProcessInput;
//import com.continuuity.api.flow.flowlet.AbstractFlowlet;
////import com.continuuity.api.flow.flowlet.ComputeFlowlet;
//import com.continuuity.api.flow.flowlet.FlowletSpecification;
////import com.continuuity.api.flow.flowlet.OutputCollector;
//import com.continuuity.api.flow.flowlet.StreamEvent;
////import com.continuuity.api.flow.flowlet.StreamsConfigurator;
////import com.continuuity.api.flow.flowlet.Tuple;
////import com.continuuity.api.flow.flowlet.TupleContext;
////import com.continuuity.api.flow.flowlet.TupleSchema;
////import com.continuuity.api.flow.flowlet.builders.TupleBuilder;
//import com.google.gson.Gson;
//import com.payvment.continuuity.entity.ProductFeedEntry;
//import com.continuuity.api.common.Bytes;
//
//
//import com.continuuity.api.flow.flowlet.*;
//
//public class ProductFeedParserFlowlet extends AbstractFlowlet {
//
//  static int numProcessed = 0;
//
//
//    OutputEmitter<ProductFeedEntry>  productFeedEntryOutputEmitter;
//
//  @Override
//   public FlowletSpecification configure() {
//      return FlowletSpecification.Builder.with()
//              .setName("product_parser")
//              .setDescription("product feed parser flowlet")
//              .build();
//
////    // Apply default stream schema to default tuple input stream
////    configurator
////        .getDefaultTupleInputStream()
////        .setSchema(TupleSchema.EVENT_SCHEMA);
////
////    // Apply internal follow-event tuple schema to output stream
////    configurator
////        .getDefaultTupleOutputStream()
////        .setSchema(ProductFeedFlow.PRODUCT_META_TUPLE_SCHEMA);
//  }
//
//
//  /** JSON parser */
//  private final Gson gson = new Gson();
//
//
//    @ProcessInput(ProductFeedFlow.inputStream)
//    public void process(StreamEvent event){
//
//    //Grab JSON string from event-stream tuple
//    String jsonEventString = new String(Bytes.toBytes(event.getBody()));
//
//    // Perform any necessary pre-processing
//    jsonEventString = preProcessSocialActionJSON(jsonEventString);
//
//    ProductFeedEntry productFeedEntry =
//            this.gson.fromJson(jsonEventString, ProductFeedEntry.class);
//
//     productFeedEntryOutputEmitter.emit(productFeedEntry);
//    }
//
////  @Override
////  public void process(Tuple tuple, TupleContext context,
////      OutputCollector collector) {
////
////    // Grab JSON string from event-stream tuple
////    String jsonEventString = new String((byte[])tuple.get("body"));
////
////    // Perform any necessar pre-processing
////    jsonEventString = preProcessSocialActionJSON(jsonEventString);
////
////    // Parse product meta JSON using GSON
////    ProductFeedEntry productMeta =
////        this.gson.fromJson(jsonEventString, ProductFeedEntry.class);
////
////    // Define and emit output tuple
////    Tuple outputTuple = new TupleBuilder().set("product", productMeta).create();
////    collector.add(outputTuple);
////  }
//
////  @Override
////  public void onSuccess(Tuple tuple, TupleContext context) {
////    numProcessed++;
////  }
//
//  static String preProcessSocialActionJSON(String jsonEventString) {
//    jsonEventString = jsonEventString.replaceFirst("@id", "product_id");
//    jsonEventString = jsonEventString.replaceFirst(
//        "\"last_modified\"", "\"date\"");
//    return jsonEventString;
//  }
//}
