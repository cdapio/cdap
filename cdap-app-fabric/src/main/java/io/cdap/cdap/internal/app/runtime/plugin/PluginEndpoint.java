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

package co.cask.cdap.internal.app.runtime.plugin;

import co.cask.cdap.api.annotation.Beta;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Type;

/**
 * Provides access to plugin endpoint. get request type of method parameter, invoke plugin method and get response.
 */
@Beta
public interface PluginEndpoint {
  /**
   * get the type of method parameter
   *
   * @return Type of method's first parameter
   * @throws IllegalArgumentException if argument list is empty
   */
  Type getMethodParameterType() throws IllegalArgumentException;

  /**
   * get the return type of plugin method
   *
   * @return return type of method
   */
  Type getResultType();

  /**
   * invoke method with the request object as parameter.
   *
   * @param reqest request object
   * @return result from invoking the object
   * @throws IOException when instantiating plugin before invoking method on it
   * @throws ClassNotFoundException when instantiating plugin before invoking method on it
   * @throws InvocationTargetException method invoke can throw
   * @throws IllegalAccessException method invoke can throw
   * @throws IllegalArgumentException method invoke can throw
   */
  Object invoke(Object reqest) throws IOException, ClassNotFoundException,
    InvocationTargetException, IllegalAccessException, IllegalArgumentException;
}
