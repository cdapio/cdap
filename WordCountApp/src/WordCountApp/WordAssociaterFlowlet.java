package WordCountApp;

import com.continuuity.api.annotation.Process;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.FlowletSpecification;

import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;

public class WordAssociaterFlowlet extends AbstractFlowlet {

  @UseDataSet("wordAssocs")
  private WordAssocTable wordAssocTable;

  public WordAssociaterFlowlet() {
    super("wordAssociater");
  }

  @Override
  public FlowletSpecification configure() {
    return FlowletSpecification.Builder.with()
      .setName(getName())
      .setDescription("Example Word Count Procedure")
      .useDataSet("wordAssocs")
      .build();
  }

  @Process("wordArrayOut")
  public void process(String [] words) throws OperationException {
    // Store word associations
    Set<String> wordSet = new TreeSet<String>(Arrays.asList(words));
    this.wordAssocTable.writeWordAssocs(wordSet);
  }
}