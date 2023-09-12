/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.worker.sidecar;

import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.internal.remote.InternalAuthenticator;
import io.cdap.cdap.proto.security.Credential;
import io.cdap.cdap.proto.security.GcpMetadataTaskContext;
import java.util.function.BiConsumer;
import javax.annotation.Nullable;

/**
 * A class which sets internal authenticated headers for the remote client using a
 * {@link GcpMetadataTaskContext} as the source.
 */
public class GcpWorkloadIdentityInternalAuthenticator implements InternalAuthenticator {

  private GcpMetadataTaskContext gcpMetadataTaskContext;

  /**
   * Constructs the {@link GcpWorkloadIdentityInternalAuthenticator}.
   */
  public GcpWorkloadIdentityInternalAuthenticator(GcpMetadataTaskContext gcpMetadataTaskContext) {
    this.gcpMetadataTaskContext = gcpMetadataTaskContext;
  }

  /**
   * Sets the {@link GcpMetadataTaskContext}.
   */
  public void setGcpMetadataTaskContext(@Nullable GcpMetadataTaskContext gcpMetadataTaskContext) {
    this.gcpMetadataTaskContext = gcpMetadataTaskContext;
  }

  /**
   * Sets internal authentication headers using a provided header setting function.
   *
   * @param headerSetter A BiConsumer header setting function used to set header values for a
   *     request.
   */
  @Override
  public void applyInternalAuthenticationHeaders(BiConsumer<String, String> headerSetter) {
    if (gcpMetadataTaskContext == null) {
      return;
    }

    if (gcpMetadataTaskContext.getUserId() != null) {
      headerSetter.accept(Constants.Security.Headers.USER_ID, gcpMetadataTaskContext.getUserId());
    }

    if (gcpMetadataTaskContext.getUserIp() != null) {
      headerSetter.accept(Constants.Security.Headers.USER_IP, gcpMetadataTaskContext.getUserIp());
    }

    if (gcpMetadataTaskContext.getUserCredential() != null) {
      Credential userCredential = gcpMetadataTaskContext.getUserCredential();
      headerSetter.accept(Constants.Security.Headers.RUNTIME_TOKEN,
          String.format("%s %s", userCredential.getType().getQualifiedName(),
              userCredential.getValue()));
    }
  }
}
