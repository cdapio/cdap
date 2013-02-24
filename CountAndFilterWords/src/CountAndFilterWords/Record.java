package CountAndFilterWords;

/**
 *
 */
public class Record {

  private final String word;
  private final String field;

  public Record(String word, String field) {
    this.word = word;
    this.field = field;
  }

  public String getWord() {
    return word;
  }

  public String getField() {
    return field;
  }

}