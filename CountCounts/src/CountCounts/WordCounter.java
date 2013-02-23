package CountCounts;

import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.OutputEmitter;

import java.nio.charset.CharacterCodingException;

public class WordCounter extends AbstractFlowlet {
  private OutputEmitter<Integer> output;

  public WordCounter() {
    super("WordCounter");
  }

  public void process(String text) throws CharacterCodingException {

    if (Common.debug) {
      System.out.println(this.getClass().getSimpleName() + ": Received event " + text);
    }

    final String delimiters = "[ .-]";
    int count = 0;
    if (text != null) {
      count = text.split(delimiters).length;
    }

    if (Common.debug) {
      System.out.println(this.getClass().getSimpleName() + ": Emitting tuple " + output);
    }
    output.emit(count);
  }

}
