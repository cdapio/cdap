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

package co.cask.cdap.api.procedure;

import java.util.Map;

/**
 * This interface defines the request to the {@link Procedure}.
 *
 * @deprecated As of version 2.6.0, replaced by 
 *             {@link co.cask.cdap.api.service.http.HttpServiceRequest HttpServiceRequest,}
 *             which is used with {@link co.cask.cdap.api.service.Service Service}
 */
@Deprecated
public interface ProcedureRequest {

  /**
   * @return Name of the method
   */
  String getMethod();

  /**
   * @return Arguments passed to the {@link Procedure}
   */
  Map<String, String> getArguments();

  /**
   * Returns an argument value provided by the key argument.
   * @param key of the argument to be retrieved
   * @return If found, returns the value associated with key; if not found, returns null.
   */
  String getArgument(String key);


  /**
   * Returns the value, cast to the <code>type</code> specified.
   * @param key of the argument to be retrieved.
   * @param type of value if found to which it will be cast.
   * @return if found, returns the value cast to <code>type</code>; else null.
   */
  <T> T getArgument(String key, Class<T> type);
}
