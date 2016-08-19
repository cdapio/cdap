/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.security;

import co.cask.cdap.common.security.ImpersonationInfo;
import co.cask.cdap.common.security.UGIProvider;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;

/**
 * A UGIProvider that always returns the current user.
 */
public class CurrentUGIProvider implements UGIProvider {

  @Override
  public UserGroupInformation getConfiguredUGI(ImpersonationInfo impersonationInfo) throws IOException {
    return UserGroupInformation.getCurrentUser();
  }
}
