package WordCountApp;

import com.continuuity.api.annotation.Output;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import com.continuuity.api.flow.flowlet.StreamEvent;

public class WordSplitterFlowlet extends AbstractFlowlet {

  public WordSplitterFlowlet() {
    super("wordSplitter");
  }
  @Output("wordOut")
  private OutputEmitter<String> wordOutput;

  @Output("wordArrayOut")
  private OutputEmitter<String[]> wordArrayOutput;

  public void process(StreamEvent event) {
    // Input is a String, need to split it by whitespace
    byte [] rawInput = Bytes.toBytes(event.getBody());
    String inputString = new String(rawInput);
    String [] words = inputString.split("\\s+");
    
    // We have an array of words, now remove all non-alpha characters
    for (int i=0; i<words.length; i++) {
      words[i] = words[i].replaceAll("[^A-Za-z]", "");
    }

    // Send the array of words to the associater
    wordArrayOutput.emit(words);
    
    // Then emit each word to the counter
    for (String word : words) {
      wordOutput.emit(word);
    }
  }
}