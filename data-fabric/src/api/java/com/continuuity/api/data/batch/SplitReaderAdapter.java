package com.continuuity.api.data.batch;


/**
 * Handy adaptor for {@link SplitReader} to convert types.
 * @param <KEY1>
 * @param <KEY>
 * @param <VALUE1>
 * @param <VALUE>
 */
public abstract class SplitReaderAdapter<KEY1, KEY, VALUE1, VALUE> extends SplitReader<KEY, VALUE> {
  private final SplitReader<KEY1, VALUE1> delegate;

  public SplitReaderAdapter(SplitReader<KEY1, VALUE1> delegate) {
    this.delegate = delegate;
  }

  protected abstract KEY convertKey(KEY1 key);
  protected abstract VALUE convertValue(VALUE1 value);

  @Override
  public void initialize(Split split) throws InterruptedException {
    delegate.initialize(split);
  }

  @Override
  public boolean nextKeyValue() throws InterruptedException {
    return delegate.nextKeyValue();
  }


  @Override
  public KEY getCurrentKey() throws InterruptedException {
    return convertKey(delegate.getCurrentKey());
  }

  @Override
  public VALUE getCurrentValue() throws InterruptedException {
    return convertValue(delegate.getCurrentValue());
  }

  @Override
  public void close() {
    delegate.close();
  }
}
