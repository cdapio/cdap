package com.payvment.continuuity;

import com.continuuity.api.flow.flowlet.ComputeFlowlet;
import com.continuuity.api.flow.flowlet.FlowletSpecifier;
import com.continuuity.api.flow.flowlet.OutputCollector;
import com.continuuity.api.flow.flowlet.Tuple;
import com.continuuity.api.flow.flowlet.TupleContext;
import com.continuuity.api.flow.flowlet.TupleSchema;
import com.continuuity.api.flow.flowlet.builders.TupleBuilder;
import com.google.gson.Gson;
import com.payvment.continuuity.entity.ProductFeedEntry;

public class ProductFeedParserFlowlet extends ComputeFlowlet {

  static int numProcessed = 0;

  @Override
  public void configure(FlowletSpecifier configurator) {
    // Apply default stream schema to default tuple input stream
    configurator
        .getDefaultFlowletInput()
        .setSchema(TupleSchema.EVENT_SCHEMA);

    // Apply internal follow-event tuple schema to output stream
    configurator
        .getDefaultFlowletOutput()
        .setSchema(ProductFeedFlow.PRODUCT_META_TUPLE_SCHEMA);
  }

  /** JSON parser */
  private final Gson gson = new Gson();

  @Override
  public void process(Tuple tuple, TupleContext context,
      OutputCollector collector) {

    // Grab JSON string from event-stream tuple
    String jsonEventString = new String((byte[])tuple.get("body"));

    // Perform any necessar pre-processing
    jsonEventString = preProcessSocialActionJSON(jsonEventString);

    // Parse product meta JSON using GSON
    ProductFeedEntry productMeta =
        this.gson.fromJson(jsonEventString, ProductFeedEntry.class);

    // Define and emit output tuple
    Tuple outputTuple = new TupleBuilder().set("product", productMeta).create();
    collector.add(outputTuple);
  }

  @Override
  public void onSuccess(Tuple tuple, TupleContext context) {
    numProcessed++;
  }

  static String preProcessSocialActionJSON(String jsonEventString) {
    jsonEventString = jsonEventString.replaceFirst("@id", "product_id");
    jsonEventString = jsonEventString.replaceFirst(
        "\"last_modified\"", "\"date\"");
    return jsonEventString;
  }
}
