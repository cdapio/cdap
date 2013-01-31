package CountTokens;

import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecifier;

public class CountTokens implements Flow {
  public void configure(FlowSpecifier specifier) {
    specifier.name("CountTokens");
    specifier.email("me@continuuity.com");
    specifier.application("End2End");
    specifier.stream("text");
    specifier.dataset(Common.tableName);
    specifier.flowlet("source", StreamSource.class, 1);
    specifier.flowlet("split", Tokenizer.class, 1);
    specifier.flowlet("upper", UpperCaser.class, 1);
    specifier.flowlet("count1", CountByField.class, 1);
    specifier.flowlet("count2", CountByField.class, 1);
    specifier.input("text", "source");
    specifier.connection("source", "split");
    specifier.connection("split", "count1");
    specifier.connection("split", "upper");
    specifier.connection("upper", "count2");
  }
}
