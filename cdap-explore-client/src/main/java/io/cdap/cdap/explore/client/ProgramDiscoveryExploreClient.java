/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.explore.client;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.security.spi.authentication.AuthenticationContext;
import com.google.inject.Inject;
import org.apache.twill.discovery.DiscoveryServiceClient;

import java.util.HashMap;
import java.util.Map;

/**
 * An Explore Client which extends {@link DiscoveryExploreClient} and is used in program container. This client is
 * aware of the {@link ProgramId} and add it to the security headers.
 */
public class ProgramDiscoveryExploreClient extends DiscoveryExploreClient {

  private final ProgramId programId;

  @Inject
  public ProgramDiscoveryExploreClient(final DiscoveryServiceClient discoveryClient,
                                       AuthenticationContext authenticationContext, ProgramId programId) {
    super(discoveryClient, authenticationContext);
    this.programId = programId;
  }

  @Override
  protected Map<String, String> addAdditionalSecurityHeaders() {
    Map<String, String> headers = new HashMap<>(super.addAdditionalSecurityHeaders());
    headers.put(Constants.Security.Headers.PROGRAM_ID, programId.toString());
    return headers;
  }
}
