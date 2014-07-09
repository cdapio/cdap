package com.continuuity.api.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Defines the strategy used when reading data from a {@link com.continuuity.api.flow.flowlet.Flowlet Flowlet's} input
 * as round-robin partitioning.
 *
 * <p>
 * The input is processed among the running {@link com.continuuity.api.flow.flowlet.Flowlet Flowlets} in a round-robin
 * manner.
 * <p>
 * To increase the throughput when a flowlet has many instances, we can specify round-robin partitioning:
 * </p>
 * 
 * <pre>
 * <code>
 * {@literal @}RoundRobin
 * {@literal @}ProcessInput("wordOut")
 * public void process(String word) throws OperationException {
 *   this.wordCountsTable.increment(Bytes.toBytes(word), 1L);
 * }
 * </code>
 * </pre>
 *
 * <p>
 * If we have 3 instances of this flowlet, every instance will receive every third word. For example, 
 * for the sequence of words in the sentence, &quot;I scream, you scream, we all scream for ice cream&quot;:
 * </p>
 *
 * <ol>
 * <li>
 * The first instance receives these words: I scream scream cream
 * </li>
 * <li>
 * The second instance receives these words: scream we for
 * </li>
 * </ol>
 * 
 * <p>
 * The potential problem with this is that both instances might attempt to increment the counter 
 * for the word &quot;scream&quot; at the same time, and that may lead to a write conflict. 
 * </p>
 *
 * <p>
 * To avoid these conflicts we can use {@link HashPartition hash partitioning}.
 * </p>
 *
 * @see HashPartition
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface RoundRobin {

}
