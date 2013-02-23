package CountAndFilterWords;

import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;

public class CountAndFilterWords implements Flow {
  @Override
  public FlowSpecification configure() {
    return FlowSpecification.Builder.with()
      .setName("CountAndFilterWords")
      .setDescription("Flow for counting words")
      .withFlowlets()
      .add("source", new StreamSource(), 1)
      .add("split-words", new Tokenizer(), 1)
      .add("upper-filter", new UpperCaseFilter(), 1)
      .add("count-all", new CountByField(), 1)
      .add("count-upper", new CountByField(), 1)
      .connect()
        .fromStream("text").to("source")
        .from("source").to("split-words")
        .from("split-words").to("count-all")
        .from("split-words").to("upper-filter")
        .from("upper-filter").to("count-upper")
      .build();
  }
//  public void configure(FlowSpecifier specifier) {
//    specifier.name("CountAndFilterWords");
//    specifier.email("me@continuuity.com");
//    specifier.application("End2End");
//    specifier.stream("text");
//    specifier.dataset(Common.counterTableName);
//    specifier.flowlet("source", StreamSource.class, 1);
//    specifier.flowlet("split-words", Tokenizer.class, 1);
//    specifier.flowlet("upper-filter", UpperCaseFilter.class, 1);
//    specifier.flowlet("count-all", CountByField.class, 1);
//    specifier.flowlet("count-upper", CountByField.class, 1);
//    specifier.input("text", "source");
//    specifier.connection("source", "split-words");
//    specifier.connection("split-words", "count-all");
//    specifier.connection("split-words", "upper-filter");
//    specifier.connection("upper-filter", "count-upper");
//  }
}
