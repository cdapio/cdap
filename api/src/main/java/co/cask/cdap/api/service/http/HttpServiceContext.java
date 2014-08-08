/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.api.service.http;

import java.util.Map;

/**
 * The context for a {@link HttpServiceHandler}. Currently contains methods to receive the
 * {@link HttpServiceSpecification} and the runtime arguments passed by the user.
 */
public interface HttpServiceContext {
  /**
   * @return the specification bound to this HttpServiceContext
   */
  HttpServiceSpecification getSpecification();

  /**
   * @return the user runtime arguments for the {@link HttpServiceHandler}s
   */
  Map<String, String> getRuntimeArguments();
}
