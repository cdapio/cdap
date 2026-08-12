/*
 * Copyright © 2024 Cask Data, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package io.cdap.cdap.datapipeline.oauth;

/**
 * The type of OAuth 2.0 authorization flow to use.
 */
public enum AuthType {
  /**
   * Standard OAuth 2.0 Authorization Code flow.
   */
  STANDARD,

  /**
   * OAuth 2.0 Authorization Code flow with Proof Key for Code Exchange (PKCE).
   * Provides enhanced security by dynamically generating a code challenge and verifier.
   */
  PKCE
}
