package WordCountApp;

import com.continuuity.api.flow.flowlet.ComputeFlowlet;
import com.continuuity.api.flow.flowlet.FlowletSpecifier;
import com.continuuity.api.flow.flowlet.OutputCollector;
import com.continuuity.api.flow.flowlet.Tuple;
import com.continuuity.api.flow.flowlet.TupleContext;
import com.continuuity.api.flow.flowlet.TupleSchema;
import com.continuuity.api.flow.flowlet.builders.TupleBuilder;

public class WordSplitterFlowlet extends ComputeFlowlet {

  @Override
  public void configure(FlowletSpecifier specifier) {
    specifier.getDefaultFlowletInput().setSchema(
        TupleSchema.EVENT_SCHEMA);
    specifier.addFlowletOutput("wordArrays").setSchema(
        WordCountFlow.SPLITTER_ASSOCIATER_SCHEMA);
    specifier.addFlowletOutput("words").setSchema(
        WordCountFlow.SPLITTER_COUNTER_SCHEMA);
  }

  @Override
  public void process(Tuple tuple, TupleContext context,
      OutputCollector collector) {
    // Input is a String, need to split it by whitespace
    byte [] rawInput = tuple.get("body");
    String inputString = new String(rawInput);
    String [] words = inputString.split("\\s+");
    
    // We have an array of words, now remove all non-alpha characters
    for (int i=0; i<words.length; i++) {
      words[i] = words[i].replaceAll("[^A-Za-z]", "");
    }

    // Send the array of words to the associater
    collector.add("wordArrays",
        new TupleBuilder().set("wordArray", words).create());
    
    // Then emit each word to the counter
    for (String word : words) {
      collector.add("words",
          new TupleBuilder().set("word", word).create());
    }
  }
}