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

package co.cask.cdap.internal.app.runtime.batch.inmemory;

import co.cask.cdap.internal.app.runtime.batch.MapReduceClassLoader;
import co.cask.cdap.internal.app.runtime.batch.MapReduceTaskContextProvider;
import com.google.inject.Injector;

/**
 * A in-memory implementation of {@link MapReduceTaskContextProvider}. It simply uses
 * the same {@link Injector} as being used by the CDAP. This class should only be used
 * by {@link MapReduceClassLoader}.
 */
public class InMemoryMapReduceTaskContextProvider extends MapReduceTaskContextProvider {

  public InMemoryMapReduceTaskContextProvider(Injector injector) {
    super(injector);
  }

  @Override
  protected void doStart() throws Exception {
    // no-op
  }

  @Override
  protected void doStop() throws Exception {
    // TODO: Flush contexts
  }
}
