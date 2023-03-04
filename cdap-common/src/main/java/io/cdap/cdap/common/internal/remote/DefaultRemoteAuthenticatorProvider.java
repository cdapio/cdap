/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.common.internal.remote;

import com.google.inject.Inject;
import com.google.inject.Provider;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.security.spi.authenticator.RemoteAuthenticator;
import javax.annotation.Nullable;
import javax.inject.Named;

/**
 * Provides a remote authenticator via the extension loader.
 */
public class DefaultRemoteAuthenticatorProvider implements Provider<RemoteAuthenticator> {

  public static final String AUTHENTICATOR_NAME_KEY = "AuthenticatorNameKey";

  @Nullable
  private final String defaultAuthenticatorName;
  private final RemoteAuthenticatorExtensionLoader remoteAuthenticatorExtensionLoader;
  private RemoteAuthenticator localRemoteAuthenticator;

  @Inject
  DefaultRemoteAuthenticatorProvider(CConfiguration cConf,
      @Named(AUTHENTICATOR_NAME_KEY) String remoteAuthenticatorNameKey,
      RemoteAuthenticatorExtensionLoader remoteAuthenticatorExtensionLoader) {
    // Use the authenticator name key for subcomponents if set, but default to overall system wide configuration key
    // if no override is set.
    String authenticatorName = cConf.get(remoteAuthenticatorNameKey);
    defaultAuthenticatorName = authenticatorName == null
        ? cConf.get(Constants.RemoteAuthenticator.REMOTE_AUTHENTICATOR_NAME)
        : authenticatorName;
    this.remoteAuthenticatorExtensionLoader = remoteAuthenticatorExtensionLoader;
    this.localRemoteAuthenticator = null;
  }

  /**
   * Retrieves the current {@link RemoteAuthenticator} from the extension loader using the
   * authenticator name or {@code null} if there is no current authenticator available.
   */
  @Override
  public RemoteAuthenticator get() {
    if (defaultAuthenticatorName == null) {
      return new NoOpRemoteAuthenticator();
    }

    // Try to get the current authenticator from the extension loader.
    RemoteAuthenticator extensionRemoteAuthenticator = remoteAuthenticatorExtensionLoader.get(
        defaultAuthenticatorName);
    if (extensionRemoteAuthenticator != null) {
      return extensionRemoteAuthenticator;
    }

    // Fall back to using a local remote authenticator if the extension remote authenticator is not found.
    // Once a local remote authenticator has been instantiated, return that instead.
    if (localRemoteAuthenticator != null) {
      return localRemoteAuthenticator;
    }

    // Check if remote authenticator binding is a local authenticator.
    RemoteAuthenticator localAuthenticator = getLocalAuthenticator(defaultAuthenticatorName);
    if (localAuthenticator != null) {
      localRemoteAuthenticator = localAuthenticator;
      return localRemoteAuthenticator;
    }

    // If no remote authenticator was found, return a NoOpRemoteAuthenticator.
    localRemoteAuthenticator = new NoOpRemoteAuthenticator();
    return localRemoteAuthenticator;
  }

  /**
   * Helper method for getting a mapped local authenticator class, or null if none exists.
   *
   * @param remoteAuthenticatorName The remote authenticator name
   * @return The local remote authenticator
   */
  @Nullable
  private static RemoteAuthenticator getLocalAuthenticator(String remoteAuthenticatorName) {
    switch (remoteAuthenticatorName) {
      case GceRemoteAuthenticator.GCE_REMOTE_AUTHENTICATOR_NAME:
        return new GceRemoteAuthenticator();
      case NoOpRemoteAuthenticator.NO_OP_REMOTE_AUTHENTICATOR_NAME:
        return new NoOpRemoteAuthenticator();
      default:
        return null;
    }
  }
}
