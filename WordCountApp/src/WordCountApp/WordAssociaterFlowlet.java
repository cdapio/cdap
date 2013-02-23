package WordCountApp;

import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;

import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;

public class WordAssociaterFlowlet extends AbstractFlowlet {

  public WordAssociaterFlowlet() {
    super("wordAssociater");
  }

  @UseDataSet("wordAssocs")
  private WordAssocTable wordAssocTable;

  public void process(String [] words) throws OperationException {
    // Store word associations
    Set<String> wordSet = new TreeSet<String>(Arrays.asList(words));
    this.wordAssocTable.writeWordAssocs(wordSet);
  }
}