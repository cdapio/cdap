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
package co.cask.cdap.etl.common;

import co.cask.cdap.api.TransactionUtil;
import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.etl.api.Lookup;
import com.google.common.base.Function;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;

/**
 * Implementation of {@link AbstractLookupProvider} that uses {@link Transactional}.
 * This class will provide a {@link Lookup} that wraps each call within a transaction
 * using {@link Transactional}, and therefore can be used when executing lookup functions outside a transaction.
 */
public class TxLookupProvider extends AbstractLookupProvider {

  private final Transactional tx;

  public TxLookupProvider(Transactional tx) {
    this.tx = tx;
  }

  @Override
  public <T> Lookup<T> provide(final String table, final Map<String, String> arguments) {
    //noinspection unchecked
    return new Lookup<T>() {

      @Override
      public T lookup(final String key) {
        return executeLookup(table, arguments, new Function<Lookup<T>, T>() {
          @Nullable
          @Override
          public T apply(Lookup<T> input) {
            return input.lookup(key);
          }
        });
      }

      @Override
      public Map<String, T> lookup(final String... keys) {
        return executeLookup(table, arguments, new Function<Lookup<T>, Map<String, T>>() {
          @Nullable
          @Override
          public Map<String, T> apply(Lookup<T> input) {
            return input.lookup(keys);
          }
        });
      }

      @Override
      public Map<String, T> lookup(final Set<String> keys) {
        return executeLookup(table, arguments, new Function<Lookup<T>, Map<String, T>>() {
          @Nullable
          @Override
          public Map<String, T> apply(Lookup<T> input) {
            return input.lookup(keys);
          }
        });
      }
    };
  }

  @Nullable
  private <T, R> R executeLookup(final String table, final Map<String, String> arguments,
                                 final Function<Lookup<T>, R> func) {
    final AtomicReference<R> result = new AtomicReference<>();
    TransactionUtil.execute(tx, new TxRunnable() {
      @Override
      public void run(DatasetContext context) throws Exception {
        Lookup<T> lookup = getLookup(table, context.getDataset(table, arguments));
        result.set(func.apply(lookup));
      }
    });
    return result.get();
  }
}
