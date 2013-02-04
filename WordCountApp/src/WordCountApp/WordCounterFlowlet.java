package WordCountApp;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.dataset.table.Increment;
import com.continuuity.api.data.dataset.table.Table;
import com.continuuity.api.flow.flowlet.ComputeFlowlet;
import com.continuuity.api.flow.flowlet.FlowletSpecifier;
import com.continuuity.api.flow.flowlet.OutputCollector;
import com.continuuity.api.flow.flowlet.Tuple;
import com.continuuity.api.flow.flowlet.TupleContext;

public class WordCounterFlowlet extends ComputeFlowlet {

  @Override
  public void configure(FlowletSpecifier specifier) {
    specifier.getDefaultFlowletInput().setSchema(
        WordCountFlow.SPLITTER_COUNTER_SCHEMA);
    specifier.getDefaultFlowletOutput().setSchema(
        UniqueCountTable.UNIQUE_COUNT_TABLE_TUPLE_SCHEMA);
  }

  private Table wordStatsTable;
  
  private Table wordCountsTable;

  private UniqueCountTable uniqueCountTable;

  @Override
  public void initialize() {
    try {
      this.wordStatsTable = getFlowletContext().getDataSet("wordStats");
      this.wordCountsTable = getFlowletContext().getDataSet("wordCounts");
      this.uniqueCountTable = getFlowletContext().getDataSet("uniqueCount");
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Override
  public void process(Tuple tuple, TupleContext context,
      OutputCollector collector) {
    String word = tuple.get("word");

    try {

      // Count number of times we have seen this word
      this.wordCountsTable.stage(
          new Increment(Bytes.toBytes(word), Bytes.toBytes("total"), 1L));
      
      // Count other word statistics (word length, total words seen)
      this.wordStatsTable.stage(
          new Increment(Bytes.toBytes("totals"),
              new byte [][] {
                Bytes.toBytes("total_length"), Bytes.toBytes("total_words")
              },
              new long [] { word.length(), 1L }));

      // For the unique counter, writing the entry to the table will create a
      // Tuple that we send to the UniqueCounter Flowlet
      Tuple outputTuple = this.uniqueCountTable.writeEntryAndCreateTuple(word);
      collector.add(outputTuple);

    } catch (OperationException e) {
      // Fail the process() call if we get an exception
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }
}