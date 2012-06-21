package CountTokens;

import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecifier;

public class CountTokenFlow implements Flow {
  public void configure(FlowSpecifier specifier) {
    specifier.name("CountTokens");
    specifier.email("me@continuuity.com");
    specifier.application("End2End");
    specifier.stream("input");
    specifier.flowlet("source", StreamSource.class, 1);
    specifier.flowlet("split", Tokenizer.class, 1);
    specifier.flowlet("upper", UpperCaser.class, 1);
    specifier.flowlet("count1", CountByField.class, 1);
    specifier.flowlet("count2", CountByField.class, 1);
    specifier.input("input", "source");
    specifier.connection("source", "split");
    specifier.connection("split", "count1");
    specifier.connection("split", "upper");
    specifier.connection("upper", "count2");
  }
}
