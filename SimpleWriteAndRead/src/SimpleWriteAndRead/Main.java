
import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecifier;

public class Main implements Flow {
  public void configure(FlowSpecifier specifier) {
    specifier.name("SimpleWriteAndRead");
    specifier.email("me@continuuity.com");
    specifier.application("End2End");
    specifier.stream("input");
    specifier.flowlet("source", StreamSource.class, 1);
    specifier.flowlet("writer", WriterFlowlet.class, 1);
    specifier.flowlet("reader", ReaderFlowlet.class, 1);
    specifier.input("input", "source");
    specifier.connection("source", "writer");
    specifier.connection("writer", "reader");
  }
}
