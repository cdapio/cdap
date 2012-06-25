package CountRandom;


import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecifier;

public class Main implements Flow {

  public void configure(FlowSpecifier specifier) {
    specifier.name("CountRandom");
    specifier.email("me@continuuity.com");
    specifier.application("CountRandom");
    specifier.flowlet("gen", RandomSource.class, 1);
    specifier.flowlet("split", NumberSplitter.class, 1);
    specifier.flowlet("count", NumberCounter.class, 1);
    specifier.connection("gen", "split");
    specifier.connection("split", "count");
  }

}
