/*
 * Copyright © 2018 Cask Data, Inc.
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
 *
 */

package co.cask.cdap.internal.provision;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.macro.InvalidMacroException;
import co.cask.cdap.api.macro.MacroEvaluator;
import co.cask.cdap.api.security.store.SecureStore;

/**
 * Evaluates the secure() macro function for provisioner properties.
 */
public class ProvisionerMacroEvaluator implements MacroEvaluator {
  public static final String SECURE_FUNCTION = "secure";
  private final String namespace;
  private final SecureStore secureStore;

  public ProvisionerMacroEvaluator(String namespace, SecureStore secureStore) {
    this.namespace = namespace;
    this.secureStore = secureStore;
  }

  @Override
  public String lookup(String property) throws InvalidMacroException {
    // this should never happen, as the parser should be configured to skip lookups
    throw new UnsupportedOperationException("Lookups are not supported in provisioners.");
  }

  @Override
  public String evaluate(String macroFunction, String... arguments) throws InvalidMacroException {
    if (!SECURE_FUNCTION.equals(macroFunction)) {
      // this should never happen, as the parser should be configured to skip all other functions
      throw new UnsupportedOperationException(String.format("%s is not a supported macro function in provisioners.",
                                                            macroFunction));
    }
    if (arguments.length != 1) {
      throw new InvalidMacroException("Secure store macro function only supports 1 argument.");
    }
    try {
      return Bytes.toString(secureStore.getSecureData(namespace, arguments[0]).get());
    } catch (Exception e) {
      throw new InvalidMacroException("Failed to resolve macro '" + macroFunction + "(" + arguments[0] + ")'", e);
    }
  }
}
