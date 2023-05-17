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

package io.cdap.cdap.internal.credential;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.cdap.cdap.common.conf.Constants.Gateway;
import io.cdap.http.AbstractHttpHandler;
import io.cdap.http.HttpHandler;
import javax.ws.rs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link HttpHandler} for credential provisioning.
 */
@Singleton
@Path(Gateway.API_VERSION_3 + "/namespaces/{namespace-id}")
public class CredentialProvisionerHttpHandler extends AbstractHttpHandler  {
  private static final Logger LOG = LoggerFactory.getLogger(CredentialProvisionerHttpHandler.class);

  @Inject
  @VisibleForTesting
  public CredentialProvisionerHttpHandler() {
  }


}
