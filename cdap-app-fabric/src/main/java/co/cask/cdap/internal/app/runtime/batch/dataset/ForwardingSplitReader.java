/*
 * Copyright © 2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.internal.app.runtime.batch.dataset;

import co.cask.cdap.api.data.batch.Split;
import co.cask.cdap.api.data.batch.SplitReader;

/**
 * A {@link SplitReader} that forwards all methods call to another {@link SplitReader}.
 *
 * @param <KEY> The key type.
 * @param <VALUE> The value type.
 */
public abstract class ForwardingSplitReader<KEY, VALUE> extends SplitReader<KEY, VALUE> {

  private final SplitReader<KEY, VALUE> delegate;

  public ForwardingSplitReader(SplitReader<KEY, VALUE> delegate) {
    this.delegate = delegate;
  }

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
    return delegate.getCurrentKey();
  }

  @Override
  public VALUE getCurrentValue() throws InterruptedException {
    return delegate.getCurrentValue();
  }

  @Override
  public void close() {
    delegate.close();
  }

  protected SplitReader<KEY, VALUE> getDelegate() {
    return delegate;
  }
}
