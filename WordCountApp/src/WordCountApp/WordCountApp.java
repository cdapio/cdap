package WordCountApp;

import com.continuuity.api.data.dataset.table.Table;
import com.continuuity.api.flow.Application;
import com.continuuity.api.flow.ApplicationSpecification;
import com.continuuity.api.data.stream.Stream;

public class WordCountApp implements Application {
  @Override
  public ApplicationSpecification configure() {
    return ApplicationSpecification.builder()
        .setApplicationName("WordCountApp")
        .addStream(new Stream("wordStream"))
        .addDataSet(new Table("wordStats"))
        .addDataSet(new Table("wordCounts"))
        .addDataSet(new UniqueCountTable("uniqueCount"))
        .addDataSet(new WordAssocTable("wordAssocs"))
        .addFlow(WordCountFlow.class)
        .addQuery(WordCountQuery.class)
        .create();
  }
}
