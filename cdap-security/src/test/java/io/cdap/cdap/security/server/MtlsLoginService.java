/*
 * Copyright © 2014 Cask Data, Inc.
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

import com.google.common.collect.Maps;
import java.io.FileInputStream;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;
import javax.servlet.ServletRequest;
import org.eclipse.jetty.security.IdentityService;
import org.eclipse.jetty.security.LoginService;
import org.eclipse.jetty.server.UserIdentity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The login service validates the user to be a known & trusted
 * user only if the client's {@link UserIdentity} is defined the
 * realm.properties file
 *
 */
public class MtlsLoginService implements LoginService {

  protected IdentityService identitySevice;
  protected String realmFilePath;
  private static final Logger LOG = LoggerFactory.getLogger(MtlsLoginService.class);
  private Map<String, String> userRolesMap;

  private Map<String, String> loadConfiguredIdentities() {
    Pattern realmUserPattern = Pattern.compile("(\\w*):(\\w*),(\\w*)");
    userRolesMap = Maps.newHashMap();
    String line = null;
    String[] tokens;

    try {
      FileInputStream fis = new FileInputStream(realmFilePath);
      Properties props = new Properties();
      props.load(fis);
      for (Object key : props.keySet()) {
        userRolesMap.put(key.toString(), props.getProperty(key.toString()).split(",")[1].toString());
      }
    } catch (Exception e) {
      LOG.error("Failed to read Realm File at : " + realmFilePath, e);
      return null;
    }

    return userRolesMap;
  }

  public MtlsLoginService(String realmFilePath) {
    this.realmFilePath = realmFilePath;
    loadConfiguredIdentities();
  }

  public void setIdentitySevice(IdentityService identitySevice) {
    this.identitySevice = identitySevice;
  }

  @Override
  public String getName() {
    return MtlsLoginService.class.getSimpleName();
  }

  @Override
  public UserIdentity login(String username, Object credentials, ServletRequest servletRequest) {
    UserIdentity identity = new MtlsUserIdentity(username, credentials);
    return identity;
  }

  @Override
  public IdentityService getIdentityService() {
    return identitySevice;
  }

  @Override
  public boolean validate(UserIdentity user) {
    if (userRolesMap.containsKey(user.getUserPrincipal().getName())) {
      return true;
    } else {
      return false;
    }

  }

  @Override
  public void logout(UserIdentity user) {
    // no-op
  }

  @Override
  public void setIdentityService(IdentityService service) {
    this.identitySevice = service;
  }

}
