package CountAndFilterWords;

import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Tokenizer extends AbstractFlowlet {
  private static Logger LOG = LoggerFactory.getLogger(Tokenizer.class);

  private OutputEmitter<Record> output;

  public Tokenizer() {
    super("split-words");
  }

  public void process(Record record) {
    final String[] fields = { "title", "text" };

    LOG.debug(this.getContext().getName() + ": Received record with word "
                + record.getWord() + " and field " + record.getField());

    for (String field : fields) {
      tokenize(record.getField(), field);
    }
  }

  void tokenize(String text, String field) {
    if (text == null) {
      return;
    }
    final String delimiters = "[ .-]";
    String[] tokens = text.split(delimiters);

    for (String token : tokens) {
      LOG.debug(this.getContext().getName() + ": Emitting record with word "
                  + token + " and field " + field);

      output.emit(new Record(token, field));
    }
  }
}
