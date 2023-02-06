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

package io.cdap.cdap.proto.security;

/**
 * Encapsulating class for security-sensitive variables.
 */
public class SecurityContext {

  private String userID;
  private Credential userCredential;
  private String userIP;

  public SecurityContext() {}

  public SecurityContext(String userID, Credential userCredential, String userIP) {
    this.userID = userID;
    this.userCredential = userCredential;
    this.userIP = userIP;
  }

  public String getUserID() {
    return userID;
  }

  public void setUserID(String userID) {
    this.userID = userID;
  }

  public Credential getUserCredential() {
    return userCredential;
  }

  public void setUserCredential(Credential userCredential) {
    this.userCredential = userCredential;
  }

  public String getUserIP() {
    return userIP;
  }

  public void setUserIP(String userIP) {
    this.userIP = userIP;
  }
}
