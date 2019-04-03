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
import co.cask.cdap.security.TokenSecureStoreRenewer;
import co.cask.cdap.security.impersonation.Impersonator;
import com.google.common.base.Throwables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillPreparer;
import org.apache.twill.yarn.YarnSecureStore;

import java.util.concurrent.TimeUnit;

/**
 * A {@link TwillPreparer} wrapper that provides impersonation support.
 */
final class ImpersonatedTwillPreparer extends ForwardingTwillPreparer {

  private final Configuration hConf;
  private final TwillPreparer delegate;
  private final Impersonator impersonator;
  private final ProgramId programId;
  private final TokenSecureStoreRenewer secureStoreRenewer;

  ImpersonatedTwillPreparer(Configuration hConf, TwillPreparer delegate, Impersonator impersonator,
                            TokenSecureStoreRenewer secureStoreRenewer, ProgramId programId) {
    this.hConf = hConf;
    this.delegate = delegate;
    this.impersonator = impersonator;
    this.programId = programId;
    this.secureStoreRenewer = secureStoreRenewer;
  }

  @Override
  public TwillPreparer getDelegate() {
    return delegate;
  }

  @Override
  public TwillController start(final long timeout, final TimeUnit timeoutUnit) {
    try {
      return impersonator.doAs(programId, () -> {
        // Add secure tokens
        if (User.isHBaseSecurityEnabled(hConf) || UserGroupInformation.isSecurityEnabled()) {
          addSecureStore(YarnSecureStore.create(secureStoreRenewer.createCredentials()));
        }
        return new ImpersonatedTwillController(delegate.start(timeout, timeoutUnit),
                                               impersonator, programId);
      });
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
