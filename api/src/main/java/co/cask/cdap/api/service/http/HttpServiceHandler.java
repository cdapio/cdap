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


/**
 * Interface for user to handle HTTP requests.
 */
public interface HttpServiceHandler {

  /**
   * Configures this HttpServiceHandler with the given {@link HttpServiceConfigurer}.
   * This method is invoked at deployment time
   * @param configurer The {@link HttpServiceConfigurer} which is used to configure this Handler
   */
  void configure(HttpServiceConfigurer configurer);

  /**
   * Initializes this HttpServiceHandler at runtime. This method is invoked only once during the startup of
   * the HttpServiceHandler.This method can be used to initialize any user related resources.
   * @param context http service runtime context
   * @throws Exception
   */
  void initialize(HttpServiceContext context) throws Exception;

  /**
   * 
   */
  void destroy();
}
