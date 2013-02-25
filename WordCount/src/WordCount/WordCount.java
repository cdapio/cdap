package WordCount;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.data.dataset.KeyValueTable;
import com.continuuity.api.data.dataset.table.Table;
import com.continuuity.api.data.stream.Stream;

public class WordCount implements Application {
  @Override
  public ApplicationSpecification configure() {
    return ApplicationSpecification.Builder.with()
      .setName("WordCount")
      .setDescription("Example Word Count Application")
      .withStreams()
        .add(new Stream("wordStream"))
      .withDataSets()
        .add(new Table("wordStats"))
        .add(new KeyValueTable("wordCounts"))
        .add(new UniqueCountTable("uniqueCount"))
        .add(new WordAssocTable("wordAssocs"))
      .withFlows()
        .add(new WordCountFlow())
      .withProcedures()
        .add(new WordCountProcedure())
      .build();
  }
}
