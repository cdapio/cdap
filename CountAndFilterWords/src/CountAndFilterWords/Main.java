package CountAndFilterWords;

import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecifier;

public class Main implements Flow {
  public void configure(FlowSpecifier specifier) {
    specifier.name("CountAndFilterWords");
    specifier.email("me@continuuity.com");
    specifier.application("End2End");
    specifier.stream("text");
    specifier.flowlet("source", StreamSource.class, 1);
    specifier.flowlet("split-words", Tokenizer.class, 1);
    specifier.flowlet("upper-filter", UpperCaseFilter.class, 1);
    specifier.flowlet("count-all", CountByField.class, 1);
    specifier.flowlet("count-upper", CountByField.class, 1);
    specifier.input("text", "source");
    specifier.connection("source", "split-words");
    specifier.connection("split-words", "count-all");
    specifier.connection("split-words", "upper-filter");
    specifier.connection("upper-filter", "count-upper");
  }
}
