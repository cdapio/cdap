package CountTokens;

import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;

public class CountTokens implements Flow {
  @Override
  public FlowSpecification configure() {
    return FlowSpecification.Builder.with()
      .setName("CountTokens")
      .setDescription("")
      .withFlowlets()
        .add("source", new StreamSource())
        .add("split", new Tokenizer())
        .add("upper", new UpperCaser())
        .add("count1", new CountByField())
        .add("count2", new CountByField())
      .connect()
        .from("source").to("split")
        .from("split").to("count1")
        .from("split").to("count1")
      .from("OddOrEven").to("count2")
      .build();
  }
}
