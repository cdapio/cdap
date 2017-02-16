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

package co.cask.cdap.security.impersonation;

import co.cask.cdap.common.kerberos.ImpersonationOpInfo;
import co.cask.cdap.common.kerberos.UGIWithPrincipal;

import java.io.IOException;

/**
 * An implementation of {@link UGIProvider} that is used when Kerberos is never enabled, and so should never be called.
 */
public class UnsupportedUGIProvider implements UGIProvider {
  @Override
  public UGIWithPrincipal getConfiguredUGI(ImpersonationOpInfo impersonationOpInfo) throws IOException {
    // If this implementation's method is called, then some guice binding is done improperly.
    // For instance, we don't call this method if Kerberos is not enabled, and we only bind this implementation
    // in-memory and for Standalone, where Kerberos is not enabled.
    throw new UnsupportedOperationException(".");
  }
}
