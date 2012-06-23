package my1stapp;


import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecifier;

public class My1stFlow implements Flow {

  public void configure(FlowSpecifier specifier) {
    specifier.name("randomcount");
    specifier.email("me@continuuity.com");
    specifier.application("my1stapp");
    specifier.flowlet("gen", RandomSource.class, 1);
    specifier.flowlet("split", NumberSplitter.class, 1);
    specifier.flowlet("count", NumberCounter.class, 1);
    specifier.connection("gen", "split");
    specifier.connection("split", "count");
  }

}
