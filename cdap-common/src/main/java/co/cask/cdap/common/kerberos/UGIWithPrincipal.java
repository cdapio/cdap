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

package co.cask.cdap.common.kerberos;

import org.apache.hadoop.security.UserGroupInformation;

/**
 * Created by rsinha on 2/15/17.
 */
public class UGIWithPrincipal {
  private final String principal;
  private final UserGroupInformation ugi;

  public UGIWithPrincipal(String principal, UserGroupInformation ugi) {
    this.principal = principal;
    this.ugi = ugi;
  }

  public String getPrincipal() {
    return principal;
  }

  public UserGroupInformation getUGI() {
    return ugi;
  }

  @Override
  public String toString() {
    return "UGIWithPrincipal{" +
      "principal='" + principal + '\'' +
      ", ugi=" + ugi +
      '}';
  }
}
