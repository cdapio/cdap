package WordCountApp;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.api.data.dataset.table.Increment;
import com.continuuity.api.data.dataset.table.Read;
import com.continuuity.api.data.dataset.table.Table;

import java.util.Iterator;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

public class WordAssocTable extends DataSet {

  private Table table;

  public WordAssocTable(String name) {
    super(name);
    this.table = new Table("word_assoc_" + name);
  }

  public WordAssocTable(DataSetSpecification spec) {
    super(spec);
    this.table = new Table(
        spec.getSpecificationFor("word_assoc_" + this.getName()));
  }

  @Override
  public DataSetSpecification configure() {
    return new DataSetSpecification.Builder(this)
        .dataset(this.table.configure())
        .create();
  }

  /**
   * Returns the top words associated with the specified word and the number
   * of times the words have appeared together
   * @param word
   * @param limit
   * @return
   * @throws OperationException
   */
  public Map<String,Long> readWordAssocs(String word, int limit)
      throws OperationException {
    // Read all columns
    OperationResult<Map<byte[], byte[]>> result =
        this.table.read(new Read(Bytes.toBytes(word), null, null));
    Map<String,Long> wordAssocs = new TreeMap<String,Long>();
    if (!result.isEmpty()) {
      for (Map.Entry<byte[],byte[]> entry : result.getValue().entrySet()) {
        String assocWord = Bytes.toString(entry.getKey());
        Long assocCount = Bytes.toLong(entry.getValue());
        wordAssocs.put(assocWord, assocCount);
      }
      if (wordAssocs.size() > limit) {
        wordAssocs = sortAndLimit(wordAssocs, limit);
      }
    }
    return wordAssocs;
  }

  private static class WordAssoc implements Comparable<WordAssoc> {
    
    final String word;
    final Long count;
    
    WordAssoc(String word, Long count) {
      this.word = word;
      this.count = count;
    }
    
    @Override
    public int compareTo(WordAssoc other) {
      if (other.count == this.count) return this.word.compareTo(other.word);
      if (this.count < other.count) return -1;
      return 1;
    }
  }

  private Map<String,Long> sortAndLimit(Map<String,Long> wordAssocs,
      int limit) {
    if (wordAssocs.size() <= limit) return new TreeMap<String,Long>(wordAssocs);
    
    // Sort the entries by count
    NavigableSet<WordAssoc> sortedWordAssocs = new TreeSet<WordAssoc>();
    for (Map.Entry<String,Long> wordAssoc : wordAssocs.entrySet()) {
      sortedWordAssocs.add(
          new WordAssoc(wordAssoc.getKey(), wordAssoc.getValue()));
    }

    // Iterate the top 'limit' entries and add them to the final map
    Iterator<WordAssoc> assocIterator = sortedWordAssocs.descendingIterator();
    Map<String,Long> finalAssocs = new TreeMap<String,Long>();
    for (int i = 0 ; i < limit ; i++) {
      WordAssoc wordAssoc = assocIterator.next();
      finalAssocs.put(wordAssoc.word, wordAssoc.count);
    }

    return finalAssocs;
  }

  /**
   * Stores associations between the specified set of words.  That is, for every
   * word in the set, an association will be stored for each of the other words
   * in the set.
   * <p>
   * This is an asynchronous write operation.
   * @param words words to store associations between
   * @throws OperationException
   */
  public void writeWordAssocs(Set<String> words) throws OperationException {
    for (String rootWord : words) {
      for (String assocWord : words) {
        if (!rootWord.equals(assocWord)) {
          writeWordAssoc(rootWord, assocWord);
        }
      }
    }
  }

  /**
   * Stores an association between the specified word and associated word.
   * <p>
   * This is an asynchronous write operation.
   * @param word root word
   * @param assocWord associated word
   * @throws OperationException
   */
  public void writeWordAssoc(String word, String assocWord)
      throws OperationException {
    this.table.write(
        new Increment(Bytes.toBytes(word), Bytes.toBytes(assocWord), 1L));
  }
}
