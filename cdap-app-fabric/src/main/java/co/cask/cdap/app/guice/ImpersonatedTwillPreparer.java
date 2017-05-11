/*
 * Copyright © 2016-2017 Cask Data, Inc.
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

import co.cask.cdap.internal.app.runtime.distributed.ForwardingTwillPreparer;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.security.impersonation.Impersonator;
import com.google.common.base.Throwables;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillPreparer;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * A {@link TwillPreparer} wrapper that provides impersonation support.
 */
final class ImpersonatedTwillPreparer extends ForwardingTwillPreparer {

  private final TwillPreparer delegate;
  private final Impersonator impersonator;
  private final ProgramId programId;

  ImpersonatedTwillPreparer(TwillPreparer delegate, Impersonator impersonator, ProgramId programId) {
    this.delegate = delegate;
    this.impersonator = impersonator;
    this.programId = programId;
  }

  @Override
  public TwillPreparer getDelegate() {
    return delegate;
  }

  @Override
  public TwillController start() {
    try {
      return impersonator.doAs(programId, new Callable<TwillController>() {
        @Override
        public TwillController call() throws Exception {
          return new ImpersonatedTwillController(delegate.start(), impersonator, programId);
        }
      });
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public TwillController start(final long timeout, final TimeUnit timeoutUnit) {
    try {
      return impersonator.doAs(programId, new Callable<TwillController>() {
        @Override
        public TwillController call() throws Exception {
          return new ImpersonatedTwillController(delegate.start(timeout, timeoutUnit), impersonator, programId);
        }
      });
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
