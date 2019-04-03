/*
 * Copyright © 2018 Cask Data, Inc.
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
 *
 */

package co.cask.cdap.internal.bootstrap.executor;

import co.cask.cdap.api.retry.RetryableException;
import co.cask.cdap.common.NamespaceAlreadyExistsException;
import co.cask.cdap.common.namespace.NamespaceAdmin;
import co.cask.cdap.common.service.RetryStrategies;
import co.cask.cdap.common.service.RetryStrategy;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.FileAlreadyExistsException;
import java.util.concurrent.TimeUnit;

/**
 * Creates the default namespace if it doesn't exist.
 */
public class DefaultNamespaceCreator extends BaseStepExecutor<EmptyArguments> {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultNamespaceCreator.class);
  private final NamespaceAdmin namespaceAdmin;

  @Inject
  DefaultNamespaceCreator(NamespaceAdmin namespaceAdmin) {
    this.namespaceAdmin = namespaceAdmin;
  }

  @Override
  public void execute(EmptyArguments arguments) throws FileAlreadyExistsException {
    try {
      if (!namespaceAdmin.exists(NamespaceId.DEFAULT)) {
        namespaceAdmin.create(NamespaceMeta.DEFAULT);
        LOG.info("Successfully created namespace '{}'.", NamespaceMeta.DEFAULT);
      }
    } catch (FileAlreadyExistsException e) {
      // avoid retrying if its a FileAlreadyExistsException
      throw e;
    } catch (NamespaceAlreadyExistsException e) {
      // default namespace already exists, move on
    } catch (Exception e) {
      // the default namespace is valid so any exception here is transient and should be retried.
      throw new RetryableException(e);
    }
  }

  @Override
  protected RetryStrategy getRetryStrategy() {
    // retry with no time limit
    return RetryStrategies.exponentialDelay(200, 10000, TimeUnit.MILLISECONDS);
  }
}
