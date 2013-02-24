package CountAndFilterWords;

import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UpperCaseFilter extends AbstractFlowlet {

  private static Logger LOG = LoggerFactory.getLogger(UpperCaseFilter.class);

  private OutputEmitter<Record> output;

  public UpperCaseFilter() {
    super("upper-filter");
  }

  public void process(Record recordIn) {
    LOG.debug(this.getContext().getName() + ": Received word " + recordIn.getWord());

    String word = recordIn.getWord();
    if (word == null) {
      return;
    }
    // filter words that are not upper-cased
    if (!Character.isUpperCase(word.charAt(0))) {
      return;
    }

    Record recordOut = new Record(word, null);

    LOG.debug(this.getContext().getName() + ": Emitting word " + recordOut.getWord());

    output.emit(recordOut);
  }
}
