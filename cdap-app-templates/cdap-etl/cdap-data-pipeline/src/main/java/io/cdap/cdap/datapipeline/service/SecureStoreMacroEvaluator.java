/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.datapipeline.service;

import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.macro.InvalidMacroException;
import io.cdap.cdap.api.macro.MacroEvaluator;
import io.cdap.cdap.api.security.store.SecureStore;
import io.cdap.cdap.api.security.store.SecureStoreData;

/**
 * Evaluates secure store macros if the key exists. Otherwise, returns the input.
 */
public class SecureStoreMacroEvaluator implements MacroEvaluator {
  private final String namespace;
  private final SecureStore secureStore;

  public SecureStoreMacroEvaluator(String namespace, SecureStore secureStore) {
    this.namespace = namespace;
    this.secureStore = secureStore;
  }

  @Override
  public String lookup(String property) throws InvalidMacroException {
    // this will get ignored by the parser
    throw new InvalidMacroException("Unable to lookup the value for " + property);
  }

  @Override
  public String evaluate(String macroFunction, String... arguments) throws InvalidMacroException {
    try {
      SecureStoreData secureStoreData = secureStore.get(namespace, arguments[0]);
      return Bytes.toString(secureStoreData.get());
    } catch (Exception e) {
      throw new InvalidMacroException("Unable to get the secure value for " + arguments[0]);
    }
  }

}
