/*
 * Copyright © 2015 Cask Data, Inc.
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

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.TxRunnable;
import co.cask.cdap.api.dataset.Dataset;

import java.nio.ByteBuffer;

/**
 * Instance of this class is for consuming HTTP request body incrementally. Methods defined in
 * {@link HttpServiceHandler} can return an instance of this class to consume request body in small chunks
 * to avoid running out of memory for requests with large body.
 *
 * Example:
 *
 * <p>
 *   <pre><code>
 *      public class MyHttpHandler extends AbstractHttpServiceHandler {
 *
 *        {@literal@}POST
 *        {@literal@}Path("/digest")
 *        public HttpContentConsumer computeDigest(HttpServiceRequest request,
 *                                                 HttpServiceResponder responder,
 *                                                 {@literal@}HeaderParam String digestType) throws Exception {
 *          if (digestType == null) {
 *            responder.sendError(400, "No message digest type is provided");
 *            return null;
 *          }
 *
 *          final MessageDigest messageDigest = MessageDigest.getInstance(digestType);
 *          return new HttpContentConsumer() {
 *
 *            {@literal@}Override
 *            public void onReceived(ByteBuffer chunk) throws Exception {
 *              messageDigest.update(chunk);
 *            }
 *
 *            {@literal@}Override
 *            public void onFinish(HttpServiceResponder responder) throws Exception {
 *              responder.sendString(Bytes.toHexString(messageDigest.digest()));
 *            }
 *
 *            {@literal@}Override
 *            public void onError(HttpServiceResponder responder, Throwable failureCause) {
 *              responder.sendError(500, failureCause.getMessage());
 *            }
 *          }
 *        }
 *      }
 *   </code></pre>
 * </p>
 */
public abstract class HttpContentConsumer {

  /**
   * This method will get invoked when a new chunk of the request body is available to be consumed.
   * It is guaranteed that no concurrent calls to this method will be made.
   * <p>
   * Access to transactional {@link Dataset Datasets} should be done through the
   * {@link Transactional#execute(TxRunnable)} method.
   * </p>
   *
   * @param chunk a {@link ByteBuffer} containing a chunk of the request body
   * @param transactional for executing a {@link TxRunnable} in a single transaction.
   * @throws Exception if there is any error when processing the received chunk
   */
  public abstract void onReceived(ByteBuffer chunk, Transactional transactional) throws Exception;

  /**
   * This method will get invoked when reached the end of the request body. It must use the given
   * {@link HttpServiceResponder} to send response in order to complete the HTTP call. This method is
   * always executed inside a single transaction.
   *
   * @param responder a {@link HttpServiceResponder} for sending response
   * @throws Exception if there is any error
   */
  public abstract void onFinish(HttpServiceResponder responder) throws Exception;

  /**
   * This method will get invoked when there is an error while processing the request body chunks. It must use the given
   * {@link HttpServiceResponder} to send response in order to complete the HTTP call.
   *
   * Any issues related to network as well as any {@link Exception Exceptions} raised
   * from either {@link #onReceived(ByteBuffer, Transactional)}
   * or {@link #onFinish(HttpServiceResponder)} methods will have this method invoked.
   *
   * This method is always executed inside a single transaction.
   *
   * @param responder a {@link HttpServiceResponder} for sending response
   * @param failureCause the reason of the failure
   */
  public abstract void onError(HttpServiceResponder responder, Throwable failureCause);
}
