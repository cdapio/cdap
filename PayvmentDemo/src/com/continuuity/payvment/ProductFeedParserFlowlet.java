package com.continuuity.payvment;

import com.continuuity.api.flow.flowlet.ComputeFlowlet;
import com.continuuity.api.flow.flowlet.OutputCollector;
import com.continuuity.api.flow.flowlet.StreamsConfigurator;
import com.continuuity.api.flow.flowlet.Tuple;
import com.continuuity.api.flow.flowlet.TupleContext;
import com.continuuity.api.flow.flowlet.TupleSchema;
import com.continuuity.api.flow.flowlet.builders.TupleBuilder;
import com.google.gson.Gson;

public class ProductFeedParserFlowlet extends ComputeFlowlet {

  @Override
  public void configure(StreamsConfigurator configurator) {
    // Apply default stream schema to default tuple input stream
    configurator
        .getDefaultTupleInputStream()
        .setSchema(TupleSchema.EVENT_SCHEMA);

    // Apply internal follow-event tuple schema to output stream
    configurator
        .getDefaultTupleOutputStream()
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

    // Parse social action JSON using GSON
    SocialAction action =
        this.gson.fromJson(jsonEventString, SocialAction.class);

    // Define and emit output tuple
    Tuple outputTuple = new TupleBuilder().set("action", action).create();
    collector.add(outputTuple);
  }

  private String preProcessSocialActionJSON(String jsonEventString) {
    return jsonEventString.replaceFirst("@id", "id");
  }

}
