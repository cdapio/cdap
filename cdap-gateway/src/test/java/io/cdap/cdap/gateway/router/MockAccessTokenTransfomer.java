/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.gateway.router;

import co.cask.cdap.security.auth.AccessTokenIdentifier;
import co.cask.cdap.security.auth.AccessTokenTransformer;

import java.io.IOException;
import java.util.LinkedHashSet;

public class MockAccessTokenTransfomer extends AccessTokenTransformer {

  public MockAccessTokenTransfomer() {
    super(null, null);
  }

  @Override
  public AccessTokenIdentifierPair transform(String accessToken) throws IOException {
    return new AccessTokenIdentifierPair("dummy", new AccessTokenIdentifier("dummy", new LinkedHashSet<String>(),
                                                                            System.currentTimeMillis(),
                                                                            System.currentTimeMillis() + 100000));
  }
}
