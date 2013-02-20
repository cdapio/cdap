package WordCountApp;

import java.io.IOException;

import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.data.dataset.table.Table;
import com.continuuity.api.procedure.AbstractProcedure;
import com.continuuity.api.procedure.ProcedureRequest;
import com.continuuity.api.procedure.ProcedureResponder;
import com.continuuity.api.procedure.ProcedureResponse;
import com.continuuity.api.procedure.ProcedureResponse.Code;
import com.continuuity.api.procedure.ProcedureSpecification;

public class WordCountProcedure extends AbstractProcedure {

  @Override
  public ProcedureSpecification configure() {
    return ProcedureSpecification.Builder.with()
        .setName("WordCountProcedure")
        .setDescription("Example Word Count Procedure")
        .build();
  }

  @UseDataSet("wordStats")
  private Table wordStatsTable;
  
  @UseDataSet("wordCounts")
  private Table wordCountsTable;
  
  @UseDataSet("uniqueCount")
  private UniqueCountTable uniqueCountTable;

  @UseDataSet("wordAssocs")
  private WordAssocTable wordAssocTable;

  public void process(ProcedureRequest request, ProcedureResponder responder)
      throws IOException {
    responder.response(new ProcedureResponse(Code.SUCCESS, "Success!"));
  }
//  @Override
//  public QueryProviderResponse process(String method,
//      Map<String, String> arguments) {
//    try {
//      if (method.equals("getStats")) {
//        // query 1 is 'getStats' and returns all the global statistics, no args
//        // wordsSeen, uniqueWords, avgWordLength
//        
//        // Read the total_length and total_words to calculate average length
//        OperationResult<Map<byte[],byte[]>> result = this.wordStatsTable.read(
//            new Read(Bytes.toBytes("total"), Bytes.toBytes("total_length")));
//        Long totalLength = result.isEmpty() ? 0L :
//          Bytes.toLong(result.getValue().get(Bytes.toBytes("total_length")));
//        result = this.wordStatsTable.read(
//            new Read(Bytes.toBytes("total"), Bytes.toBytes("total_words")));
//        Long totalWords = result.isEmpty() ? 0L :
//          Bytes.toLong(result.getValue().get(Bytes.toBytes("total_words")));
//        Double avgLength = new Double(totalLength) / new Double(totalWords);
//        
//        // Read the unique word count
//        Long uniqueWords = this.uniqueCountTable.readUniqueCount();
//        
//        // Construct and return the JSON string
//        String ret = "{'wordsSeen':" + totalWords + ",'uniqueWords':" +
//              uniqueWords + ",'avgLength':" + avgLength + "}";
//        return new QueryProviderResponse(ret);
//        
//      } else if (method.equals("getCount")) {
//        // query 2 is 'getCount' with argument of word='', optional limit=#
//        // returns count of word and top words associated with that word,
//        // up to specified limit (default limit = 10 if not specified) 
//        if (!arguments.containsKey("word")) {
//          String msg = "Method 'getCount' requires argument 'word'";
//          getQueryProviderContext().getLogger().error(msg);
//          return new QueryProviderResponse(Status.FAILED, msg            , "");
//        }
//        
//        // Parse the arguments
//        String word = arguments.get("word");
//        Integer limit = arguments.containsKey("limit") ?
//            Integer.valueOf(arguments.get("limit")) : 10;
//            
//        // Read the word count
//        OperationResult<Map<byte[],byte[]>> result = this.wordCountsTable.read(
//            new Read(Bytes.toBytes(word), Bytes.toBytes("total")));
//        Long wordCount = result.isEmpty() ? 0L :
//          Bytes.toLong(result.getValue().get(Bytes.toBytes("total")));
//          
//        // Read the top associated words
//        Map<String,Long> wordsAssocs =
//            this.wordAssocTable.readWordAssocs(word, limit);
//        
//        // Construct and return the JSON string
//        StringBuilder builder = new StringBuilder("{'word':'" + word +
//            "','count':" + wordCount + "," + "'assocs':[");
//        boolean first = true;
//        for (Map.Entry<String, Long> wordAssoc : wordsAssocs.entrySet()) {
//          if (!first) builder.append(",");
//          else first = false;
//          builder.append("{'word':'" + wordAssoc.getKey() + "',");
//          builder.append("'count':" + wordAssoc.getValue() + "}");
//        }
//        builder.append("]}");
//        return new QueryProviderResponse(builder.toString());
//        
//      } else {
//        getQueryProviderContext().getLogger().error("Invalid method: " + method);
//        return new QueryProviderResponse(Status.FAILED,
//            "Invalid method: " + method, "");
//      }
//    } catch (OperationException e) {
//      getQueryProviderContext().getLogger().error("Exception during query", e);
//      return new QueryProviderResponse(Status.FAILED,
//          "Exception occurred: " + e.getLocalizedMessage(), "");
//    }
//  }

}
