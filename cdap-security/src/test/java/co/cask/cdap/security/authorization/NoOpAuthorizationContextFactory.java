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

package co.cask.cdap.security.authorization;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.data.DatasetInstantiationException;
import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.common.test.NoopAdmin;
import co.cask.cdap.security.auth.context.AuthenticationTestContext;
import co.cask.cdap.security.spi.authorization.AuthorizationContext;
import co.cask.cdap.security.store.DummySecureStore;
import org.apache.tephra.TransactionFailureException;

import java.util.Map;
import java.util.Properties;

/**
 * A no-op implementation of {@link AuthorizationContextFactory} for use in tests.
 */
public class NoOpAuthorizationContextFactory implements AuthorizationContextFactory {
  @Override
  public AuthorizationContext create(Properties extensionProperties) {
    return new DefaultAuthorizationContext(extensionProperties, new NoOpDatasetContext(), new NoopAdmin(),
                                           new NoOpTransactional(), new AuthenticationTestContext(),
                                           new DummySecureStore());
  }

  private static final class NoOpTransactional implements Transactional {
    @Override
    public void execute(TxRunnable runnable) throws TransactionFailureException {
      // no-op
    }

    @Override
    public void execute(int timeoutInSeconds, TxRunnable runnable) throws TransactionFailureException {
      // no-op
    }
  }

  private static class NoOpDatasetContext implements DatasetContext {
    @Override
    public <T extends Dataset> T getDataset(String name) throws DatasetInstantiationException {
      throw new DatasetInstantiationException("NoOpDatasetContext cannot instantiate datasets");
    }

    @Override
    public <T extends Dataset> T getDataset(String namespace, String name) throws DatasetInstantiationException {
      throw new DatasetInstantiationException("NoOpDatasetContext cannot instantiate datasets");
    }

    @Override
    public <T extends Dataset> T getDataset(String name, Map<String, String> arguments)
      throws DatasetInstantiationException {
      throw new DatasetInstantiationException("NoOpDatasetContext cannot instantiate datasets");
    }

    @Override
    public <T extends Dataset> T getDataset(String namespace, String name, Map<String, String> arguments)
      throws DatasetInstantiationException {
      throw new DatasetInstantiationException("NoOpDatasetContext cannot instantiate datasets");
    }

    @Override
    public void releaseDataset(Dataset dataset) {
      // no-op
    }

    @Override
    public void discardDataset(Dataset dataset) {
      // no-op
    }
  }
}
