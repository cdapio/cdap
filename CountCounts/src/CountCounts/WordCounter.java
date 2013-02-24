package CountCounts;

import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WordCounter extends AbstractFlowlet {
  private static Logger LOG = LoggerFactory.getLogger(WordCounter.class);

  private OutputEmitter<Integer> output;

  public WordCounter() {
    super("count");
  }

  public void process(String text) {

    LOG.debug(this.getContext().getName() + ": Received text " + text);


    final String delimiters = "[ .-]";
    int count = 0;
    if (text != null) {
      count = text.split(delimiters).length;
    }

    LOG.debug(this.getContext().getName() + ": Emitting count " + output);

    output.emit(count);
  }

}
