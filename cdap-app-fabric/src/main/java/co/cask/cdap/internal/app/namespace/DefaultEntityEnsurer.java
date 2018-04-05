/*
 * Copyright © 2015-2017 Cask Data, Inc.
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
import co.cask.cdap.common.NamespaceAlreadyExistsException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.namespace.NamespaceAdmin;
import co.cask.cdap.common.service.RetryOnStartFailureService;
import co.cask.cdap.common.service.RetryStrategies;
import co.cask.cdap.internal.app.store.profile.ProfileStore;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProfileId;
import co.cask.cdap.proto.profile.Profile;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.Service;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.FileAlreadyExistsException;
import java.util.concurrent.TimeUnit;

/**
 * Thread that ensures that the default entities exists
 */
public final class DefaultEntityEnsurer extends AbstractService {


  private static final Logger LOG = LoggerFactory.getLogger(DefaultEntityEnsurer.class);

  private final Service serviceDelegate;

  @Inject
  public DefaultEntityEnsurer(NamespaceAdmin namespaceAdmin, ProfileStore profileStore) {
    this.serviceDelegate = new RetryOnStartFailureService(() -> new AbstractService() {
      @Override
      protected void doStart() {
        boolean failed = false;
        Exception failureException = null;
        try {
          if (!namespaceAdmin.exists(NamespaceId.DEFAULT)) {
            namespaceAdmin.create(NamespaceMeta.DEFAULT);
            // if there is no exception, assume successfully created and break
            LOG.info("Successfully created namespace '{}'.", NamespaceMeta.DEFAULT);
          }
        } catch (FileAlreadyExistsException e) {
          LOG.warn("Got exception while trying to create namespace '{}'.", NamespaceMeta.DEFAULT, e);
          // avoid retrying if its a FileAlreadyExistsException
        } catch (NamespaceAlreadyExistsException e) {
          // default namespace already exists
          LOG.info("Default namespace already exists.");
        } catch (Exception e) {
          failed = true;
          failureException = e;
        }

        try {
          try {
            profileStore.getProfile(ProfileId.DEFAULT);
          } catch (NotFoundException e) {
            profileStore.add(ProfileId.DEFAULT, Profile.DEFAULT);
          }
        } catch (AlreadyExistsException e) {
          // don't expect this to happen, but it already exists so we're ok.
        } catch (Exception e) {
          failed = true;
          if (failureException == null) {
            failureException = e;
          } else {
            failureException.addSuppressed(e);
          }
        }

        if (failed) {
          notifyFailed(failureException);
        } else {
          notifyStarted();
        }
      }

      @Override
      protected void doStop() {
        notifyStopped();
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
