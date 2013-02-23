package CountCounts;

import com.continuuity.api.annotation.Handle;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.procedure.AbstractProcedure;
import com.continuuity.api.procedure.ProcedureRequest;
import com.continuuity.api.procedure.ProcedureResponder;
import com.continuuity.api.procedure.ProcedureResponse;
import com.continuuity.api.procedure.ProcedureSpecification;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Map;

/**
 *
 */
public class CountQuery extends AbstractProcedure {

  CounterTable counters;

  public CountQuery() {
    super("CountQuery");
  }

  public ProcedureSpecification configure() {
    return ProcedureSpecification.Builder.with()
      .setName("CountQuery")
      .setDescription("Example Count Query Procedure")
      .build();
  }

  @Handle("countQuery")
  public void handle(ProcedureRequest request, ProcedureResponder responder) throws OperationException, IOException {
    final Map<String, String> arguments = request.getArguments();
    String method=request.getMethod();
    long count = 0;
    if ("total".equals(method)) {
      String what = request.getArguments().get("key");
      if ("sink".equals(what)) {
        count = this.counters.get(Incrementer.keyTotal);
      }
      else if ("source".equals(what)) {
        count = this.counters.get(StreamSource.keyTotal);
      }
      else {
        responder.stream(new ProcedureResponse(ProcedureResponse.Code.FAILURE));
        return;
      }
    }
    else if ("count".equals(method)) {
      String key = arguments.get("words");
      if (key != null) {
        try {
          Integer.parseInt(key);
        } catch (NumberFormatException e) {
          key = null;
        }
      }
      if (key == null) {
        responder.stream(new ProcedureResponse(ProcedureResponse.Code.FAILURE));
      }
      count = this.counters.get(key);
    }
    ProcedureResponse.Writer writer = responder.stream(new ProcedureResponse(ProcedureResponse.Code.SUCCESS));
    try {
      writer.write(ByteBuffer.wrap(Long.toString(count).getBytes(Charset.forName("UTF-8"))));
    } finally {
      writer.close();
    }
  }
}
