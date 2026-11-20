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

package io.cdap.cdap.internal.namespace.credential;

import com.google.common.util.concurrent.Service;
import io.cdap.cdap.proto.credential.CredentialProvisioningException;
import io.cdap.cdap.proto.credential.NamespaceCredentialProvider;
import io.cdap.cdap.proto.credential.NotFoundException;
import io.cdap.cdap.proto.credential.ProvisionedCredential;
import java.io.IOException;
import javax.annotation.Nullable;

/**
 * A service which provides credentials based on identity and profile associated with the namespace.
 */
public interface NamespaceCredentialProviderService extends NamespaceCredentialProvider, Service {

  /**
   * Provisions a short-lived credential for the system P4SA identity in {@code NamespaceId.SYSTEM}.
   */
  ProvisionedCredential provisionSystemP4sa(@Nullable String scopes)
      throws CredentialProvisioningException, IOException, NotFoundException;
}
