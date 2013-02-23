package CountTokens;

import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.OutputEmitter;

import java.util.Map;

public class UpperCaser extends AbstractFlowlet {
  private OutputEmitter<String> output;

  public UpperCaser() {
    super("upper");
  }

  public void process(Map<String, String> tupleIn) {
    if (Common.debug)
      System.out.println(this.getClass().getSimpleName() + ": Received tuple " + tupleIn);

    String word = tupleIn.get("word");
    if (word == null) return;
    String upper = word.toUpperCase();

    if (Common.debug)
      System.out.println(this.getClass().getSimpleName() + ": Emitting word " + upper);

    output.emit(upper);
  }
}
