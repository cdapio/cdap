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

package co.cask.cdap.api.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotates a {@link co.cask.cdap.api.flow.flowlet.Flowlet Flowlet's} method to indicate that it will process
 * its input in batch.
 * 
 * <p>
 * By default, a Flowlet processes a single data object at a time within a single transaction. To increase throughput, 
 * you can process a batch of data objects within the same transaction: 
 * </p>
 *
 * <p>
 * <pre><code>
 * {@literal @}Batch(100)
 * {@literal @}ProcessInput
 * public void process(Iterator{@literal <}String> words) {
 *   ...
 * }
 * </code></pre>
 * </p>
 * 
 * <p>
 * In this example, 100 data objects are dequeued at one time and processed within a single transaction.
 * Note that the signature of the method in the above example has an {@link java.util.Iterator} over the input type.
 * </p>
 *
 * <p>
 * You could also keep the argument as an individual input type:
 * </p>
 *
 * <p>
 * <pre><code>
 * {@literal @}Batch(100)
 * {@literal @}ProcessInput
 * public void process(String word) {
 *   ...
 * }
 * </code></pre>
 *
 * By doing so, the process method will be called repeatedly by the system for each input in the dequeued batch within
 * single transaction.
 * </p>
 *
 * <p>
 * The batch size can also be controlled through flowlet properties or runtime arguments. You can specify the key to use
 * for looking up the batch size value with a {@code @Batch} annotation.
 * </p>
 *
 * <p>
 * <pre><code>
 * {@literal @}Batch(key = "batch.size", value = 100)
 * {@literal @}ProcessInput
 * public void process(String word) {
 *   ...
 * }
 * </code></pre>
 *
 * By specifying the {@code key} element in the {@code @Batch} annotation, the runtime system will resolve the
 * batch size in this order (highest precedence first):
 *
 * <ol>
 *   <li>Runtime argument with name = {@code flowlet.<flowletName>.<key>}</li>
 *   <li>Runtime argument with name = {@code flowlet.*.<key>}</li>
 *   <li>Runtime argument with name = {@code <key>}</li>
 *   <li>Flowlet properties with name = {@code <key>}</li>
 *   <li>The {@code value} element specified in the {@code @Batch} annotation</li>
 * </ol>
 * </p>
 * 
 * <p>
 * If you use batch processing, your transactions can take longer and the probability of a conflict due
 * to a failed process increases (see {@link HashPartition hash partitioning}).
 * </p>
 * 
 * <p>
 * See the <i><a href="http://docs.cask.co/cdap/current/en/developers-manual/index.html">CDAP Developers' Manual</a></i>
 * for more information.
 * </p>
 *
 * @see HashPartition
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface Batch {
  /**
   * Declare the maximum number of objects that can be processed in a batch.
   */
  int value();

  /**
   * Declare the key to use for looking up batch size value from the runtime arguments or the flowlet properties.
   * If the given key cannot be found in neither of them, then the value returned by
   * {@link #value()} will be used.
   */
  String key() default "";
}
