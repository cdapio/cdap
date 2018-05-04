/*
 * Copyright © 2016-2018 Cask Data, Inc.
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

package co.cask.cdap.app.guice;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.internal.app.runtime.distributed.ProgramTwillApplication;
import co.cask.cdap.internal.app.runtime.distributed.TwillAppNames;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.security.TokenSecureStoreRenewer;
import co.cask.cdap.security.impersonation.Impersonator;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.RunId;
import org.apache.twill.api.SecureStoreUpdater;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillPreparer;
import org.apache.twill.api.TwillRunnable;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.api.security.SecureStoreRenewer;
import org.apache.twill.api.security.SecureStoreWriter;
import org.apache.twill.common.Cancellable;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * A {@link TwillRunnerService} wrapper that provides impersonation support.
 */
final class ImpersonatedTwillRunnerService implements TwillRunnerService {

  private final Configuration hConf;
  private final TwillRunnerService delegate;
  private final Impersonator impersonator;
  private final TokenSecureStoreRenewer secureStoreRenewer;

  ImpersonatedTwillRunnerService(Configuration hConf, TwillRunnerService delegate, Impersonator impersonator,
                                 TokenSecureStoreRenewer secureStoreRenewer) {
    this.hConf = hConf;
    this.delegate = delegate;
    this.impersonator = impersonator;
    this.secureStoreRenewer = secureStoreRenewer;
  }

  @Override
  public void start() {
    delegate.start();
  }

  @Override
  public void stop() {
    delegate.stop();
  }

  @Override
  public TwillPreparer prepare(TwillRunnable runnable) {
    // CDAP doesn't use this method, hence not impersonating
    return delegate.prepare(runnable);
  }

  @Override
  public TwillPreparer prepare(TwillRunnable runnable, ResourceSpecification resourceSpecification) {
    // CDAP doesn't use this method, hence not impersonating
    return delegate.prepare(runnable, resourceSpecification);
  }

  @Override
  public TwillPreparer prepare(TwillApplication application) {
    if (application instanceof ProgramTwillApplication) {
      ProgramId programId = ((ProgramTwillApplication) application).getProgramRunId().getParent();
      return new ImpersonatedTwillPreparer(hConf, delegate.prepare(application), impersonator,
                                           secureStoreRenewer, programId);
    }
    return delegate.prepare(application);
  }

  @Override
  public TwillController lookup(String applicationName, RunId runId) {
    TwillController controller = delegate.lookup(applicationName, runId);
    if (isMasterService(applicationName)) {
      return controller;
    }
    try {
      return new ImpersonatedTwillController(controller, impersonator, TwillAppNames.fromTwillAppName(applicationName));
    } catch (IllegalArgumentException e) {
      // If the conversion from twill app name to programId failed, don't wrap
      return controller;
    }
  }

  @Override
  public Iterable<TwillController> lookup(String applicationName) {
    return wrapControllers(delegate.lookup(applicationName), applicationName);
  }

  @Override
  public Iterable<LiveInfo> lookupLive() {
    return Iterables.transform(delegate.lookupLive(), new Function<LiveInfo, LiveInfo>() {
      @Override
      public LiveInfo apply(final LiveInfo liveInfo) {
        return new LiveInfo() {
          @Override
          public String getApplicationName() {
            return liveInfo.getApplicationName();
          }

          @Override
          public Iterable<TwillController> getControllers() {
            return wrapControllers(liveInfo.getControllers(), liveInfo.getApplicationName());
          }
        };
      }
    });
  }

  @Override
  public Cancellable scheduleSecureStoreUpdate(SecureStoreUpdater updater, long initialDelay,
                                               long delay, TimeUnit unit) {
    throw new UnsupportedOperationException("The scheduleSecureStoreUpdate method is deprecated, " +
                                              "it shouldn't be used.");
  }

  @Override
  public Cancellable setSecureStoreRenewer(SecureStoreRenewer renewer, long initialDelay,
                                           long delay, long retryDelay, TimeUnit unit) {
    return delegate.setSecureStoreRenewer(wrapSecureStoreRenewer(renewer), initialDelay, delay, retryDelay, unit);
  }

  private Iterable<TwillController> wrapControllers(Iterable<TwillController> controllers, String applicationName) {
    if (isMasterService(applicationName)) {
      return controllers;
    }

    try {
      final ProgramId programId = TwillAppNames.fromTwillAppName(applicationName);
      return Iterables.transform(controllers, new Function<TwillController, TwillController>() {
        @Override
        public TwillController apply(TwillController controller) {
          return new ImpersonatedTwillController(controller, impersonator, programId);
        }
      });
    } catch (IllegalArgumentException e) {
      // If the conversion from twill app name to programId failed, don't wrap
      return controllers;
    }
  }

  private SecureStoreRenewer wrapSecureStoreRenewer(final SecureStoreRenewer renewer) {
    return new SecureStoreRenewer() {
      @Override
      public void renew(final String application, final RunId runId,
                        final SecureStoreWriter secureStoreWriter) throws IOException {
        if (isMasterService(application)) {
          renewer.renew(application, runId, secureStoreWriter);
          return;
        }

        ProgramId programId;
        try {
          programId = TwillAppNames.fromTwillAppName(application);
        } catch (IllegalArgumentException e) {
          // If the conversion from twill app name to programId failed, just delegate
          renewer.renew(application, runId, secureStoreWriter);
          return;
        }

        try {
          // Impersonate as the program owner and call the renewer
          impersonator.doAs(programId, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
              renewer.renew(application, runId, secureStoreWriter);
              return null;
            }
          });
        } catch (Exception e) {
          // it should already be a runtime exception anyways, since none of the methods in the above callable
          // throw any checked exceptions
          throw Throwables.propagate(e);
        }
      }
    };
  }

  private boolean isMasterService(String applicationName) {
    return Constants.Service.MASTER_SERVICES.equals(applicationName);
  }
}
