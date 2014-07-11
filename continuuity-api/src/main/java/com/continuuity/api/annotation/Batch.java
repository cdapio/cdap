package com.continuuity.api.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotates a {@link com.continuuity.api.flow.flowlet.Flowlet Flowlet's} method to indicate that it will process
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
 * </p>
 * 
 * <p>
 * By doing so, the process method will be called repeatedly by the system for each input in the dequeued batch within
 * single transaction.
 * </p>
 *
 * <p>
 * If you use batch processing, your transactions can take longer and the probability of a conflict due
 * to a failed process increases (see {@link HashPartition hash partitioning}).
 * </p>
 * 
 * <p>
 * See the <i><a href="http://continuuity.com/docs/reactor/current/en/">Continuuity Reactor Developer Guides</a></i>
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
}
