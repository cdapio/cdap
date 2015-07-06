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

package co.cask.cdap.internal.app.namespace;

import co.cask.cdap.common.AlreadyExistsException;
import co.cask.cdap.common.NamespaceCannotBeCreatedException;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.service.RetryOnStartFailureService;
import co.cask.cdap.common.service.RetryStrategies;
import com.google.common.base.Supplier;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.Service;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Thread that ensures that the default namespace exists
 */
public final class DefaultNamespaceEnsurer extends AbstractService {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultNamespaceEnsurer.class);

  private final Service serviceDelegate;

  @Inject
  public DefaultNamespaceEnsurer(final NamespaceAdmin namespaceAdmin) {
    this.serviceDelegate = new RetryOnStartFailureService(new Supplier<Service>() {
      @Override
      public Service get() {
        return new AbstractService() {
          @Override
          protected void doStart() {
            try {
              namespaceAdmin.createNamespace(Constants.DEFAULT_NAMESPACE_META);
              // if there is no exception, assume successfully created and break
              LOG.info("Created default namespace successfully.");
              notifyStarted();
            } catch (AlreadyExistsException e) {
              // default namespace already exists
              LOG.info("Default namespace already exists.");
              notifyStarted();
            } catch (NamespaceCannotBeCreatedException e) {
              notifyFailed(e);
            }
          }

          @Override
          protected void doStop() {
            notifyStopped();
          }
        };
      }
    }, RetryStrategies.exponentialDelay(200, 5000, TimeUnit.MILLISECONDS));
  }

  @Override
  protected void doStart() {
    serviceDelegate.start();
    notifyStarted();
  }

  @Override
  protected void doStop() {
    serviceDelegate.stop();
    notifyStopped();
  }
}
