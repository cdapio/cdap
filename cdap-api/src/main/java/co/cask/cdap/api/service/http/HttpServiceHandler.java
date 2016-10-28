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

import co.cask.cdap.api.ProgramLifecycle;
import co.cask.cdap.api.annotation.TransactionControl;
import co.cask.cdap.api.annotation.TransactionPolicy;

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
 * <p>
 * Example:
 * <p>
 *   <pre><code>
 *      public class MyHttpHandler implements HttpServiceHandler {
 *
 *        {@literal @}GET
 *        {@literal @}Path("/ping")
 *        public void process(HttpServiceRequest request, HttpServiceResponder responder) {
 *          responder.sendString("Hello World");
 *        }
 *
 *        {@literal @}Override
 *        public void configure(HttpServiceConfigurer configurer) { }
 *
 *        {@literal @}Override
 *        public void initialize(HttpServiceContext context) throws Exception { }
 *
 *        {@literal @}Override
 *        public void destroy() { }
 *      }
 *   </code></pre>
 * <p>
 * To handle HTTP request with large body, it is better to have the handler method to return
 * a {@link HttpContentConsumer} to avoid running out of memory. Similarly, to return a response
 * with a large body, it is preferable to return respond with {@link HttpContentProducer}.
 *
 * By default, all handler methods are executed within an implicit transaction, that is, you
 * can access datasets and perform transactional data operations from the handler method.
 * In some cases, for example, if the handler does not need to perform data operations, or if
 * the time it takes to complete all operations may exceed the transaction timeout, it is
 * better to annotate the method with a different transaction policy, for example:
 * <p>
 *        {@literal @}GET
 *        {@literal @}Path("/ping")
 *        {@literal @}TransactionPolicy(TransactionControl.EXPLICIT)
 *        public void process(HttpServiceRequest request, HttpServiceResponder responder) {
 *          getContext().execute(60, new TxRunnable() {
 *            // perform data operations
 *          });
 *          responder.sendString("Hello World");
 *        }
 * </p>
 *
 * @see HttpContentConsumer
 * @see HttpContentProducer
 */
public interface HttpServiceHandler extends ProgramLifecycle<HttpServiceContext> {

  /**
   * Configures this HttpServiceHandler with the given {@link HttpServiceConfigurer}.
   * This method is invoked at deployment time.
   *
   * @param configurer the HttpServiceConfigurer which is used to configure this Handler
   */
  void configure(HttpServiceConfigurer configurer);

  /**
   * Invoked whenever a new instance of this HttpServiceHandler is created. Note that this
   * can happen at any time, because handler instances expire after a period of inactivity
   * and are recreated when there is need for a new handler to serve an incoming request.
   * That means that the time it takes to initialize adds to the time it takes to serve the
   * request. It is therefore recommended to keep this method very lightweight.
   */
  @Override
  @TransactionPolicy(TransactionControl.IMPLICIT)
  void initialize(HttpServiceContext context) throws Exception;

  /**
   * Invoked whenever an instance of this HttpServiceHandler is destroyed. This may happen
   * when the service is shut down, or when a handler instance expires due to inactivity.
   */
  @Override
  @TransactionPolicy(TransactionControl.IMPLICIT)
  void destroy();
}
