/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.data2.util.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Mutation;

import java.io.IOException;
import java.util.List;

/**
 * A concrete class implementation that delegate all {@link BufferedMutator}
 * operations to another {@link BufferedMutator}.
 */
public class DelegatingBufferedMutator implements BufferedMutator {

  private final BufferedMutator delegate;

  public DelegatingBufferedMutator(BufferedMutator delegate) {
    this.delegate = delegate;
  }

  public BufferedMutator getDelegate() {
    return delegate;
  }

  @Override
  public TableName getName() {
    return getDelegate().getName();
  }

  @Override
  public Configuration getConfiguration() {
    return getDelegate().getConfiguration();
  }

  @Override
  public void mutate(Mutation mutation) throws IOException {
    getDelegate().mutate(mutation);
  }

  @Override
  public void mutate(List<? extends Mutation> mutations) throws IOException {
    getDelegate().mutate(mutations);
  }

  @Override
  public void close() throws IOException {
    getDelegate().close();
  }

  @Override
  public void flush() throws IOException {
    getDelegate().flush();
  }

  @Override
  public long getWriteBufferSize() {
    return getDelegate().getWriteBufferSize();
  }
}
