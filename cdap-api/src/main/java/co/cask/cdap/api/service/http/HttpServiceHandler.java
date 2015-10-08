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

package co.cask.cdap.api.service.http;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;

/**
 * Interface to be implemented to handle HTTP requests. This interface can be implemented and passed along to
 * custom user services. Classes that implement this interface can add methods with the {@link Path @Path} annotation
 * to specify the endpoint which that method handles. It can also use the {@link GET @GET}, {@link POST @POST},
 * {@link PUT @PUT}, {@link DELETE @DELETE} annotations to specify the type of HTTP requests that it handles.
 *
 * Example:
 * <p>
 *   <pre><code>
 *      public class MyHttpHanlder implements HttpServiceHandler {
 *
 *        {@literal@}GET
 *        {@literal@}Path("/ping")
 *        public void process(HttpServiceRequest request, HttpServiceResponder responder) {
 *          responder.sendString("Hello World");
 *        }
 *
 *        {@literal@}Override
 *        public void configure(HttpServiceConfigurer configurer) { }
 *
 *        {@literal@}Override
 *        public void initialize(HttpServiceContext context) throws Exception { }
 *
 *        {@literal@}Override
 *        public void destroy() { }
 *      }
 *   </code></pre>
 * </p>
 *
 * To handle HTTP request with large body, it's is better to have the handler method to return
 * a {@link HttpContentConsumer} to avoid running out of memory.
 *
 * @see HttpContentConsumer
 */
public interface HttpServiceHandler {

  /**
   * Configures this HttpServiceHandler with the given {@link HttpServiceConfigurer}.
   * This method is invoked at deployment time.
   *
   * @param configurer the HttpServiceConfigurer which is used to configure this Handler
   */
  void configure(HttpServiceConfigurer configurer);

  /**
   * Invoked when the Custom User Service using this HttpServiceHandler is initialized.
   * This method can be used to initialize any user related resources at runtime.
   *
   * @param context the HTTP service runtime context
   * @throws Exception
   */
  void initialize(HttpServiceContext context) throws Exception;

  /**
   * Invoked after the Custom User Service using this HttpServiceHandler is destroyed.
   * Use this method to perform any necessary cleanup.
   */
  void destroy();
}
