package WordCountApp;


import com.continuuity.api.data.stream.Stream;
import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;

/**
 * Flow that takes any arbitrary string of input and performs word statistics.
 * <p>
 * Flow parses input string into individual words, then performs per-word counts
 * and other calculations like total number of words seen, average length
 * of words seen, unique words seen, and also tracks the words most often
 * associated with each other word.
 * <p>
 * The first Flowlet is the WordSplitter which splits the sentence into
 * individual words, cleans up non-alpha characters, and then sends the
 * sentences on to the WordAssociater and the words on to the WordCounter.
 * <p>
 * The next Flowlet is the WordAssociater that will track word associations
 * between all of the words within the input string.
 * <p>
 * The next Flowlet is the WordCounter which performs the necessary data
 * operations to do the word count and count other word statistics.
 * <p>
 * The last Flowlet is the UniqueCounter which will calculate and update the
 * unique number of words seen.
 */
public class WordCountFlow implements Flow {
  @Override
  public FlowSpecification configure() {
    return FlowSpecification.Builder.with()
        .setName("WordCountFlow")
        .setDescription("Example Word Count Flow")
        .withFlowlets()
            .add(new WordSplitterFlowlet()).apply()
            .add(new WordCounterFlowlet()).apply()
            .add(new WordAssociaterFlowlet()).apply()
            .add(new UniqueCounterFlowlet()).apply()
        .connect()
            .from(new Stream("wordStream")).to(new WordSplitterFlowlet())
            .from(new WordSplitterFlowlet()).to(new WordCounterFlowlet())
            .from(new WordSplitterFlowlet()).to(new WordAssociaterFlowlet())
            .from(new WordCounterFlowlet()).to(new UniqueCounterFlowlet())
        .build();
  }
}
