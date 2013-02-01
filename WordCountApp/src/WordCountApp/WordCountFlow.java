package WordCountApp;


import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecifier;
import com.continuuity.api.flow.flowlet.TupleSchema;
import com.continuuity.api.flow.flowlet.builders.TupleSchemaBuilder;

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
 * The last Flowlet is the UniqueCounter which will take the special Tuple
 * output from the WordCounter and update the unique number of words seen.
 */
public class WordCountFlow implements Flow {
  @Override
  public void configure(FlowSpecifier specifier) {
    // Specify meta data fields
    specifier.name("WordCounter");
    specifier.email("demo@continuuity.com");
    specifier.application("WordCountApp");

    // Specify all the dataset.
    specifier.dataset("wordStats");
    specifier.dataset("wordCounts");
    specifier.dataset("uniqueCount");
    specifier.dataset("wordAssocs");

    // Specify flowlets
    specifier.flowlet("Splitter", WordSplitterFlowlet.class, 1);
    specifier.flowlet("Counter", WordCounterFlowlet.class, 1);
    specifier.flowlet("Associater", WordAssociaterFlowlet.class, 1);
    specifier.flowlet("UniqueCounter", UniqueCounterFlowlet.class, 1);

    // Specify input stream
    specifier.input("wordStream", "Splitter");

    // Connect flowlets
    specifier.connection("Splitter", "wordArrays", "Associater", "in");
    specifier.connection("Splitter", "words", "Counter", "in");
    specifier.connection("Counter", "UniqueCounter");
  }

  // The FlowletInput/FlowletOutput TupleSchemas
  
  // Splitter -> Associater Schema is ('wordArray',String[])
  public static final TupleSchema SPLITTER_ASSOCIATER_SCHEMA =
      new TupleSchemaBuilder()
          .add("wordArray", String[].class)
          .create();

  // Splitter -> Counter Schema is ('word',String)
  public static final TupleSchema SPLITTER_COUNTER_SCHEMA =
      new TupleSchemaBuilder()
          .add("word", String.class)
          .create();
}
