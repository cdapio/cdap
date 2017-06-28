/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.data2.datafabric.dataset.service.executor;

import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.security.impersonation.Impersonator;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.Callable;

/**
 * A {link DatasetAdmin} that executes operations, while impersonating.
 */
class ImpersonatingDatasetAdmin implements DatasetAdmin {

  private static final Logger LOG = LoggerFactory.getLogger(ImpersonatingDatasetAdmin.class);

  private final DatasetAdmin delegate;
  private final Impersonator impersonator;
  private final DatasetId datasetId;

  ImpersonatingDatasetAdmin(DatasetAdmin delegate, Impersonator impersonator, DatasetId datasetId) {
    this.delegate = delegate;
    this.impersonator = impersonator;
    this.datasetId = datasetId;
  }

  @Override
  public boolean exists() throws IOException {
    return execute(new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return delegate.exists();
      }
    }, false);
  }

  @Override
  public void create() throws IOException {
    execute(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        delegate.create();
        return null;
      }
    }, false);
  }

  @Override
  public void drop() throws IOException {
    execute(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        delegate.drop();
        return null;
      }
    }, true);
  }

  @Override
  public void truncate() throws IOException {
    execute(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        delegate.truncate();
        return null;
      }
    }, false);
  }

  @Override
  public void upgrade() throws IOException {
    execute(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        delegate.upgrade();
        return null;
      }
    }, false);
  }

  @Override
  public void close() throws IOException {
    execute(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        delegate.close();
        return null;
      }
    }, false);
  }

  // helper method to execute a callable, while declaring only IOException as being thrown
  private <T> T execute(final Callable<T> callable, boolean isDelete) throws IOException {
    try {
      return isDelete ? impersonator.deleteEntity(datasetId, callable) : impersonator.doAs(datasetId, callable);
    } catch (IOException ioe) {
      throw ioe;
    } catch (Exception t) {
      Throwables.propagateIfPossible(t);

      // since the callables we execute only throw IOException (besides unchecked exceptions),
      // this should never happen
      LOG.warn("Unexpected exception while executing dataset admin operation in namespace {}.", datasetId, t);
      // the only checked exception that the Callables in this class is IOException, and we handle that in the previous
      // catch statement. So, no checked exceptions should be wrapped by the following statement. However, we need it
      // because ImpersonationUtils#doAs declares 'throws Exception', because it can throw other checked exceptions
      // in the general case
      throw Throwables.propagate(t);
    }
  }
}
