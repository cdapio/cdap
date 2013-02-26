package com.continuuity.examples.wordcount;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.DataSet;
import com.continuuity.api.data.DataSetSpecification;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.api.data.dataset.table.Increment;
import com.continuuity.api.data.dataset.table.Read;
import com.continuuity.api.data.dataset.table.Table;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

public class AssociationTable extends DataSet {

  private Table table;

  public AssociationTable(String name) {
    super(name);
    this.table = new Table("word_assoc_" + name);
  }

  public AssociationTable(DataSetSpecification spec) {
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
          this.table.write(new Increment(Bytes.toBytes(rootWord), Bytes.toBytes(assocWord), 1L));
        }
      }
    }
  }

  /**
   * Returns the top words associated with the specified word and the number
   * of times the words have appeared together
   * @param word the word of interest
   * @param limit the number of associations to return, at most
   * @return a map of the top associated words to their co-occurrence count
   * @throws OperationException
   */
  public Map<String,Long> readWordAssocs(String word, int limit)
    throws OperationException {

    // Retrieve all columns of the word’s row
    OperationResult<Map<byte[], byte[]>> result =
      this.table.read(new Read(Bytes.toBytes(word), null, null));
    TopKCollector collector = new TopKCollector(limit);
    if (!result.isEmpty()) {
      // iterate over all columns
      for (Map.Entry<byte[],byte[]> entry : result.getValue().entrySet()) {
        collector.add(Bytes.toLong(entry.getValue()),
                      Bytes.toString(entry.getKey()));
      }
    }
    return collector.getTopK();
  }

  public long getAssoc(String word1, String word2) throws OperationException {
    OperationResult<Map<byte[], byte[]>> result =
      this.table.read(new Read(Bytes.toBytes(word1), Bytes.toBytes(word2)));
    if (result.isEmpty()) {
      return 0;
    }
    byte[] count = result.getValue().get(Bytes.toBytes(word2));
    if (count == null || count.length != Bytes.SIZEOF_LONG) {
      return 0;
    }
    return Bytes.toLong(count);
  }
}

class TopKCollector {

  class Entry implements Comparable<Entry> {
    final long count;
    final String word;

    Entry(long count, String word) {
      this.count = count;
      this.word = word;
    }

    @Override
    public int compareTo(Entry other) {
      return Long.signum(count - other.count);
    }
  }

  final int limit;
  TreeSet<Entry> entries = new TreeSet<Entry>();

  TopKCollector(int limit) {
    this.limit = limit;
  }

  void add(long count, String word) {
    if (entries.size() < limit) {
      entries.add(new Entry(count, word));
    } else {
      if (entries.first().count < count) {
        entries.pollFirst();
        entries.add(new Entry(count, word));
      }
    }
  }

  Map<String, Long> getTopK() {
    TreeMap<String, Long> topK = new TreeMap<String, Long>();
    for (int i = 0; i < limit; i++) {
      Entry entry = entries.pollLast();
      if (entry == null) {
        break;
      } else {
        topK.put(entry.word, entry.count);
      }
    }
    return topK;
  }
}

