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

package co.cask.cdap.internal.app.runtime.artifact;

import co.cask.cdap.common.service.RetryOnStartFailureService;
import co.cask.cdap.common.service.RetryStrategies;
import com.google.common.base.Supplier;
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.Service;
import com.google.inject.Inject;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Thread that scans the local filesystem for system artifacts and adds them to the artifact repository if they
 * are not already there.
 */
public final class SystemArtifactLoader extends AbstractService {
  private final Service serviceDelegate;

  @Inject
  public SystemArtifactLoader(final ArtifactRepository artifactRepository) {
    this.serviceDelegate = new RetryOnStartFailureService(new Supplier<Service>() {
      @Override
      public Service get() {
        return new AbstractService() {
          @Override
          protected void doStart() {
            try {
              artifactRepository.addSystemArtifacts();
              // if there is no exception, all good, continue on
              notifyStarted();
            } catch (WriteConflictException e) {
              // transient error, fail it and retry
              notifyFailed(e);
            } catch (IOException e) {
              // transient error, fail it and retry
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
