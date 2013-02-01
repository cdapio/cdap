package WordCountApp;

import java.util.Map;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.api.data.dataset.table.Read;
import com.continuuity.api.data.dataset.table.Table;
import com.continuuity.api.query.QueryProvider;
import com.continuuity.api.query.QueryProviderContentType;
import com.continuuity.api.query.QueryProviderResponse;
import com.continuuity.api.query.QueryProviderResponse.Status;
import com.continuuity.api.query.QuerySpecifier;

public class WordCountQuery extends QueryProvider {

  @Override
  public void configure(QuerySpecifier specifier) {
    specifier.provider(WordCountQuery.class);
    specifier.service("wordcount");
    specifier.timeout(10000);
    specifier.type(QueryProviderContentType.JSON);
    specifier.dataset("wordStats");
    specifier.dataset("wordCounts");
    specifier.dataset("uniqueCount");
    specifier.dataset("wordAssocs");
  }

  private Table wordStatsTable;
  
  private Table wordCountsTable;
  
  private UniqueCountTable uniqueCountTable;

  private WordAssocTable wordAssocTable;

  @Override
  public void initialize() {
    try {
      this.wordStatsTable = getQueryProviderContext().getDataSet("wordStats");
      this.wordCountsTable = getQueryProviderContext().getDataSet("wordCounts");
      this.uniqueCountTable = getQueryProviderContext().getDataSet("uniqueCount");
      this.wordAssocTable = getQueryProviderContext().getDataSet("wordAssocs");
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Override
  public QueryProviderResponse process(String method,
      Map<String, String> arguments) {
    try {
      if (method.equals("getStats")) {
        // query 1 is 'getStats' and returns all the global statistics, no args
        // wordsSeen, uniqueWords, avgWordLength
        
        // Read the total_length and total_words to calculate average length
        OperationResult<Map<byte[],byte[]>> result = this.wordStatsTable.read(
            new Read(Bytes.toBytes("total"), Bytes.toBytes("total_length")));
        Long totalLength = result.isEmpty() ? 0L :
          Bytes.toLong(result.getValue().get(Bytes.toBytes("total_length")));
        result = this.wordStatsTable.read(
            new Read(Bytes.toBytes("total"), Bytes.toBytes("total_words")));
        Long totalWords = result.isEmpty() ? 0L :
          Bytes.toLong(result.getValue().get(Bytes.toBytes("total_words")));
        Double avgLength = new Double(totalLength) / new Double(totalWords);
        
        // Read the unique word count
        Long uniqueWords = this.uniqueCountTable.readUniqueCount();
        
        // Construct and return the JSON string
        String ret = "{'wordsSeen':" + totalWords + ",'uniqueWords':" +
              uniqueWords + ",'avgLength':" + avgLength + "}";
        return new QueryProviderResponse(ret);
        
      } else if (method.equals("getCount")) {
        // query 2 is 'getCount' with argument of word='', optional limit=#
        // returns count of word and top words associated with that word,
        // up to specified limit (default limit = 10 if not specified) 
        if (!arguments.containsKey("word")) {
          String msg = "Method 'getCount' requires argument 'word'";
          getQueryProviderContext().getLogger().error(msg);
          return new QueryProviderResponse(Status.FAILED, msg            , "");
        }
        
        // Parse the arguments
        String word = arguments.get("word");
        Integer limit = arguments.containsKey("limit") ?
            Integer.valueOf(arguments.get("limit")) : 10;
            
        // Read the word count
        OperationResult<Map<byte[],byte[]>> result = this.wordCountsTable.read(
            new Read(Bytes.toBytes(word), Bytes.toBytes("total")));
        Long wordCount = result.isEmpty() ? 0L :
          Bytes.toLong(result.getValue().get(Bytes.toBytes("total")));
          
        // Read the top associated words
        Map<String,Long> wordsAssocs =
            this.wordAssocTable.readWordAssocs(word, limit);
        
        // Construct and return the JSON string
        StringBuilder builder = new StringBuilder("{'word':'" + word +
            "','count':" + wordCount + "," + "'assocs':[");
        boolean first = true;
        for (Map.Entry<String, Long> wordAssoc : wordsAssocs.entrySet()) {
          if (!first) builder.append(",");
          else first = false;
          builder.append("{'word':'" + wordAssoc.getKey() + "',");
          builder.append("'count':" + wordAssoc.getValue() + "}");
        }
        builder.append("]}");
        return new QueryProviderResponse(builder.toString());
        
      } else {
        getQueryProviderContext().getLogger().error("Invalid method: " + method);
        return new QueryProviderResponse(Status.FAILED,
            "Invalid method: " + method, "");
      }
    } catch (OperationException e) {
      getQueryProviderContext().getLogger().error("Exception during query", e);
      return new QueryProviderResponse(Status.FAILED,
          "Exception occurred: " + e.getLocalizedMessage(), "");
    }
  }

}
