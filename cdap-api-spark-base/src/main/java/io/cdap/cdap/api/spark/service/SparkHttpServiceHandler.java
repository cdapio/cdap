/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.api.spark.service;

import co.cask.cdap.api.ProgramLifecycle;
import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.annotation.TransactionControl;
import co.cask.cdap.api.annotation.TransactionPolicy;
import co.cask.cdap.api.spark.ExtendedSparkConfigurer;
import co.cask.cdap.api.spark.Spark;
import co.cask.cdap.api.spark.SparkMain;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;

/**
 * Interface to be implemented to handle HTTP requests to the Spark driver process.
 * This interface can be implemented and passed along to {@link ExtendedSparkConfigurer#addHandlers(Iterable)}.
 * If a {@link Spark} program contains one or more {@link SparkHttpServiceHandler}, then the Spark program
 * would become a long running process until explicitly stopped.
 *
 * Classes that implement this interface can add methods with the {@link Path @Path} annotation
 * to specify the endpoint which that method handles. It can also use the {@link GET @GET}, {@link POST @POST},
 * {@link PUT @PUT}, {@link DELETE @DELETE} annotations to specify the type of HTTP requests that it handles.
 *
 * <p>
 * Example:
 * <p>
 *   <pre><code>
 *      public class MySparkHttpHandler extends AbstractSparkHttpServiceHandler {
 *
 *        {@literal @}GET
 *        {@literal @}Path("/ping")
 *        public void process(HttpServiceRequest request, HttpServiceResponder responder) {
 *          Integer result = getContext().getJavaSparkContext()
 *            .parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
 *            .reduce((v1, v2) -> v1 + v2)
 *          responder.sendString(result.toString());
 *        }
 *      }
 *   </code></pre>
 * <p>
 * To handle HTTP request with large body, it is better to have the handler method to return
 * a {@link SparkHttpContentConsumer} to avoid running out of memory. Similarly, to return a response
 * with a large body, it is preferable to return respond with {@link SparkHttpContentProducer}.
 *
 * The transaction behavior of the handler method is the same as in the Spark driver main method, that is,
 * transaction will be automatically created and committed when performing RDD/DataFrame operations that
 * involve Datasets, but the handler method itself won't be called with transaction. Multiple operations can
 * be done in the same transaction explicitly by using the {@link SparkHttpServiceContext#execute(TxRunnable)} method.
 * See {@link SparkMain} for more detail.
 *
 * @see SparkMain
 * @see HttpContentConsumer
 * @see HttpContentProducer
 */
public interface SparkHttpServiceHandler extends ProgramLifecycle<SparkHttpServiceContext> {

  /**
   * Initializes the service handler.
   * This method will be called only once per {@link SparkHttpServiceHandler} instance.
   *
   * @param context An instance of {@link SparkHttpServiceContext}
   * @throws Exception If there is any error during initialization.
   */
  @TransactionPolicy(TransactionControl.EXPLICIT)
  @Override
  void initialize(SparkHttpServiceContext context) throws Exception;

  /**
   * This method is called when the http service is about to shutdown.
   * Cleanup job can be done in this method.
   */
  @TransactionPolicy(TransactionControl.EXPLICIT)
  @Override
  void destroy();
}
