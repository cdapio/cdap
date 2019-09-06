/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.common.twill;

import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.RunId;
import org.apache.twill.api.SecureStoreUpdater;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillPreparer;
import org.apache.twill.api.TwillRunnable;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.api.security.SecureStoreRenewer;
import org.apache.twill.common.Cancellable;

import java.util.Collections;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * A no-op implementation of {@link TwillRunnerService}.
 */
public final class NoopTwillRunnerService implements TwillRunnerService {

  @Override
  public void start() {
    // no-op
  }

  @Override
  public void stop() {
    // no-op
  }

  @Override
  public TwillPreparer prepare(TwillRunnable runnable) {
    return new NoopTwillPreparer();
  }

  @Override
  public TwillPreparer prepare(TwillRunnable runnable, ResourceSpecification resourceSpecification) {
    return new NoopTwillPreparer();
  }

  @Override
  public TwillPreparer prepare(TwillApplication application) {
    return new NoopTwillPreparer();
  }

  @Nullable
  @Override
  public TwillController lookup(String applicationName, RunId runId) {
    return null;
  }

  @Override
  public Iterable<TwillController> lookup(String applicationName) {
    return Collections.emptyList();
  }

  @Override
  public Iterable<LiveInfo> lookupLive() {
    return Collections.emptyList();
  }

  @Override
  public Cancellable scheduleSecureStoreUpdate(SecureStoreUpdater updater,
                                               long initialDelay, long delay, TimeUnit unit) {
    return () -> { };
  }

  @Override
  public Cancellable setSecureStoreRenewer(SecureStoreRenewer renewer, long initialDelay,
                                           long delay, long retryDelay, TimeUnit unit) {
    return () -> { };
  }
}
