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

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.exception.AlreadyExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Thread that ensures that the default namespace exists
 */
public final class DefaultNamespaceEnsurer implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultNamespaceEnsurer.class);

  private final NamespaceAdmin namespaceAdmin;
  private final int waitBetweenRetries;
  private final TimeUnit waitUnit;

  public DefaultNamespaceEnsurer(NamespaceAdmin namespaceAdmin, int waitBetweenRetries, TimeUnit waitUnit) {
    this.namespaceAdmin = namespaceAdmin;
    this.waitBetweenRetries = waitBetweenRetries;
    this.waitUnit = waitUnit;
  }

  @Override
  public void run() {
    Thread.currentThread().setName("default-namespace-ensurer");
    int retries = 0;
    while (true) {
      try {
        namespaceAdmin.createNamespace(Constants.DEFAULT_NAMESPACE_META);
        // if there is no exception, assume successfully created and break
        LOG.info("Created default namespace successfully.");
        break;
      } catch (AlreadyExistsException e) {
        // default namespace already exists, break the retry loop
        LOG.info("Default namespace already exists.");
        break;
      } catch (Exception e) {
        retries++;
        LOG.warn("Error during retry# {} - {}", retries, e.getMessage());
        try {
          waitUnit.sleep(waitBetweenRetries);
        } catch (InterruptedException e1) {
          LOG.warn("Interrupted during retry# {} - {}", retries, e1.getMessage());
        }
      }
    }
  }
}
