package CountCounts;


import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;

public class CountCounts implements Flow {

  @Override
  public FlowSpecification configure() {
    return FlowSpecification.Builder.with()
      .setName("CountCounts")
      .setDescription("Flow for counting words")
      .withFlowlets()
        .add("source", new StreamSource())
        .add("count", new WordCounter())
        .add("tick", new Incrementer())
      .connect()
        .fromStream("text").to("source")
        .from("source").to("count")
        .from("count").to("tick")
      .build();
  }
}