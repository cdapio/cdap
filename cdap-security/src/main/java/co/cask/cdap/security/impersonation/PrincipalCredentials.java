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

package co.cask.cdap.security.impersonation;

/**
 * A wrapper around kerberos principal and credentials file path of a user to allow creating UGI remotely.
 */
public class PrincipalCredentials {
  private final String principal;
  private final String credentialsPath;


  public PrincipalCredentials(String principal, String credentialsPath) {
    this.principal = principal;
    this.credentialsPath = credentialsPath;
  }

  public String getPrincipal() {
    return principal;
  }

  public String getCredentialsPath() {
    return credentialsPath;
  }

  @Override
  public String toString() {
    return "PrincipalCredentials{" +
      "principal='" + principal + '\'' +
      ", credentialsPath=" + credentialsPath +
      '}';
  }
}
