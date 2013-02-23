package CountCounts;

import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.OutputEmitter;

public class WordCounter extends AbstractFlowlet {
  private OutputEmitter<Integer> output;

  public WordCounter() {
    super("count");
  }

  public void process(String text) {

    if (Common.debug) {
      System.out.println(this.getClass().getSimpleName() + ": Received text " + text);
    }

    final String delimiters = "[ .-]";
    int count = 0;
    if (text != null) {
      count = text.split(delimiters).length;
    }

    if (Common.debug) {
      System.out.println(this.getClass().getSimpleName() + ": Emitting count " + output);
    }
    output.emit(count);
  }

}
