/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.security.server;

import org.eclipse.jetty.util.log.Log;
import org.eclipse.jetty.util.log.Logger;

import java.util.Hashtable;
import java.util.Map;

import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.directory.Attributes;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.naming.ldap.InitialLdapContext;
import javax.naming.ldap.LdapContext;

/** LDAPLoginService
 *
 * Class extending JAASLoginService to add support for server case conversion
 */
public class LDAPLoginService extends JAASLoginService {
  private static final Logger LOG = Log.getLogger(LDAPLoginService.class);

  @Override
  public String serverCaseConvert(String userName) {
    try {
      Hashtable<String, String> map = new Hashtable(configuration.getAppConfigurationEntry(
                    io.cdap.cdap.security.server.LDAPLoginModule.class.getName())[0].getOptions());
      map.put(Context.INITIAL_CONTEXT_FACTORY, map.get("contextFactory"));
      map.put(Context.SECURITY_AUTHENTICATION, map.get("authenticationMethod"));
      map.put(Context.PROVIDER_URL, getLDAPURL(map));
      map.put(Context.SECURITY_PRINCIPAL, map.get("bindDn"));
      map.put(Context.SECURITY_CREDENTIALS, map.get("bindPassword"));

      if (map.containsKey("useLdaps") && Boolean.parseBoolean(map.get("useLdaps"))) {
        map.put("java.naming.ldap.factory.socket", LDAPLoginModule.TrustAllSSLSocketFactory.class.getName());
      }

      LdapContext ctx = new InitialLdapContext(map, null);

      SearchControls constraints = new SearchControls();
      constraints.setSearchScope(SearchControls.SUBTREE_SCOPE);

      String ldapHandlerBinding = ctx.getEnvironment().get("userBaseDn").toString();

      String filterExpr = "(" + ctx.getEnvironment().get("userRdnAttribute").toString() + "=" + userName + ")";

      NamingEnumeration answer = ctx.search(ldapHandlerBinding, filterExpr, constraints);

      if (answer.hasMore()) {
        Attributes attrs = ((SearchResult) answer.next()).getAttributes();
        userName = (String) attrs.get(ctx.getEnvironment().get("userRdnAttribute").toString()).get();
        LOG.debug("applying case adjustment using data from LDAP, updated username:: '{}'", userName);
      } else {
        LOG.warn("case adjustment not done for username, user '{}' not found in LDAP", userName);
      }
    } catch (Exception e) {
      LOG.warn("Exception occurred while get user details from LDAP, not able to apply case adjustment "
                    , e);
    }
    return userName;
  }

  private String getLDAPURL(Map<String, String> map) {
    StringBuilder sb = new StringBuilder();
    if (map.containsKey("useLdaps") && Boolean.parseBoolean(map.get("useLdaps"))) {
        sb.append("ldaps");
    } else {
      sb.append("ldap");
    }
    sb.append("://");
    sb.append(map.get("hostname")).append(":").append(map.get("port"));

    return sb.toString();
  }

}
