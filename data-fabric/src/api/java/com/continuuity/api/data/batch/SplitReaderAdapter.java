package com.continuuity.api.data.batch;


/**
 * Handy adaptor for {@link SplitReader} to convert types.
 * @param <FROM_KEY>
 * @param <TO_KEY>
 * @param <FROM_VALUE>
 * @param <TO_VALUE>
 */
public abstract class SplitReaderAdapter<FROM_KEY, TO_KEY, FROM_VALUE, TO_VALUE> extends SplitReader<TO_KEY, TO_VALUE> {
  private final SplitReader<FROM_KEY, FROM_VALUE> delegate;

  public SplitReaderAdapter(SplitReader<FROM_KEY, FROM_VALUE> delegate) {
    this.delegate = delegate;
  }

  protected abstract TO_KEY convertKey(FROM_KEY key);
  protected abstract TO_VALUE convertValue(FROM_VALUE value);

  private TO_KEY nextKey = null;
  private TO_VALUE nextValue = null;

  @Override
  public void initialize(Split split) throws InterruptedException {
    delegate.initialize(split);
    nextKey = null;
    nextValue = null;
  }

  @Override
  public boolean nextKeyValue() throws InterruptedException {
    boolean hasNext = delegate.nextKeyValue();
    if (hasNext) {
      nextKey = convertKey(delegate.getCurrentKey());
      nextValue = convertValue(delegate.getCurrentValue());
    }
    return hasNext;
  }

  @Override
  public TO_KEY getCurrentKey() throws InterruptedException {
    return nextKey;
  }

  @Override
  public TO_VALUE getCurrentValue() throws InterruptedException {
    return nextValue;
  }

  @Override
  public void close() {
    delegate.close();
    nextKey = null;
    nextValue = null;
  }
}
