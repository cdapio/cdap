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
package co.cask.cdap.security.server;

import com.google.common.base.Splitter;
import org.eclipse.jetty.security.MappedLoginService.KnownUser;
import org.eclipse.jetty.server.UserIdentity;
import org.eclipse.jetty.util.security.Credential;

import java.security.Principal;
import java.util.Map;

import javax.security.auth.Subject;
import javax.security.auth.x500.X500Principal;

/**
 * An Implementation of User Identity. All Users that provide a client
 * certificate for authentication will have their identity based on the CN of
 * the provided certficate
 *
 */
public class MTLSUserIdentity implements UserIdentity {
  private String userName;
  private Object credentials;

  private static final String PRINCIPAL_CANONICAL_NAME = "CN";

  /**
   * Extract the Canonical Name from a {@link X500Principal} name
   *
   * @param principal
   * @return
   */
  private String getX509PrincipalCN(String principal) {
    Map<String, String> principalAttributes = Splitter.on(",").withKeyValueSeparator("=")
      .split(principal.replaceAll("\\s", ""));
    if (principalAttributes.containsKey(PRINCIPAL_CANONICAL_NAME)) {
      return principalAttributes.get(PRINCIPAL_CANONICAL_NAME);
    } else {
      return null;
    }
  }

  public MTLSUserIdentity(String userName, Object credentials) {
    this.userName = userName;
    this.credentials = credentials;
  }

  public Subject getSubject() {
    Subject subject = new Subject();
    subject.getPrincipals().add(getUserPrincipal());
    subject.getPublicCredentials().add(credentials);
    subject.setReadOnly();
    return subject;
  }

  public Principal getUserPrincipal() {
    Credential credential = (credentials instanceof Credential) ? (Credential) credentials
      : Credential.getCredential(credentials.toString());
    return new KnownUser(getX509PrincipalCN(userName), credential);
  }

  // For the test not implementing role checks
  public boolean isUserInRole(String role, Scope scope) {
    return true;
  }

}
