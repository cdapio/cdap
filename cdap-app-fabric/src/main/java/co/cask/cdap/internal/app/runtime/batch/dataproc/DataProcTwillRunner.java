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
 */

package co.cask.cdap.internal.app.runtime.batch.dataproc;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.RunId;
import org.apache.twill.api.SecureStoreUpdater;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillPreparer;
import org.apache.twill.api.TwillRunnable;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.api.security.SecureStoreRenewer;
import org.apache.twill.common.Cancellable;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.apache.twill.internal.RunIds;
import org.apache.twill.internal.SingleRunnableApplication;
import org.apache.twill.internal.io.LocationCache;
import org.apache.twill.internal.io.NoCachingLocationCache;
import org.apache.twill.yarn.YarnTwillRunnerService;

import java.io.File;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class DataProcTwillRunner implements TwillRunner {

  private final SSHConfig sshConfig;
  private final LocationFactory locationFactory;
  private final Configuration hConf;

  private volatile String jvmOptions = null;

  public DataProcTwillRunner(SSHConfig sshConfig, Configuration hConf, LocationFactory locationFactory) {
    this.sshConfig = sshConfig;
    this.hConf = hConf;
//    this.locationFactory = new LocalLocationFactory(new File("/tmp/dptr"));
    this.locationFactory = locationFactory;
  }

  // Note: CDAP doesn't use the following two methods.
  @Override
  public TwillPreparer prepare(TwillRunnable runnable) {
    return prepare(runnable, ResourceSpecification.BASIC);
  }

  @Override
  public TwillPreparer prepare(TwillRunnable runnable, ResourceSpecification resourceSpecification) {
    return prepare(new SingleRunnableApplication(runnable, resourceSpecification));
  }

  @Override
  public TwillPreparer prepare(TwillApplication twillApplication) {
//    Preconditions.checkState(serviceDelegate.isRunning(), "Service not start. Please call start() first.");
    final TwillSpecification twillSpec = twillApplication.configure();
    RunId runId = RunIds.generate();
    Location appLocation = new LocalLocationFactory(new File(System.getProperty("java.io.tmpdir")))
      .create(String.format("/%s/%s", twillSpec.getName(), runId.getId()));
    LocationCache locationCache = new NoCachingLocationCache(appLocation); // TODO: do it?

    return new DataProcTwillPreparer(new Configuration(hConf), twillSpec, runId, appLocation,
                                     jvmOptions, locationCache,
                                     sshConfig, locationFactory);
  }

  @Override
  public TwillController lookup(String s, RunId runId) {
    return null;
  }

  @Override
  public Iterable<TwillController> lookup(String s) {
    return null;
  }

  @Override
  public Iterable<LiveInfo> lookupLive() {
    return null;
  }

  @Override
  public Cancellable scheduleSecureStoreUpdate(SecureStoreUpdater secureStoreUpdater,
          long l, long l1, TimeUnit timeUnit) {
    return null;
  }

  @Override
  public Cancellable setSecureStoreRenewer(SecureStoreRenewer secureStoreRenewer,
          long l, long l1, long l2, TimeUnit timeUnit) {
    return null;
  }

  /**
   * This methods sets the extra JVM options that will be passed to the java command line for every application
   * started through this {@link YarnTwillRunnerService} instance. It only affects applications that are started
   * after options is set.
   *
   * This is intended for advance usage. All options will be passed unchanged to the java command line. Invalid
   * options could cause application not able to start.
   *
   * @param options extra JVM options.
   */
  public void setJVMOptions(String options) {
    Preconditions.checkArgument(options != null, "JVM options cannot be null.");
    this.jvmOptions = options;
  }
}
