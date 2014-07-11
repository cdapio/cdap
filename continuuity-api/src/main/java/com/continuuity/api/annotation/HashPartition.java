package com.continuuity.api.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Defines the strategy used when reading data from a {@link com.continuuity.api.flow.flowlet.Flowlet Flowlet's} input
 * as hash partitioning.
 *
 * <p>
 * The input is partitioned among the running {@link com.continuuity.api.flow.flowlet.Flowlet Flowlets} using the
 * partitioning key. If the partitioning key is not emitted, then the input is treated as part of partition zero.
 * </p>
 * 
 * <p>
 * If we have 3 instances of a Flowlet, with round-robin partitioning, every instance will receive every third word. 
 * For example, for the sequence of words in the sentence, &quot;I scream, you scream, we all scream for ice 
 * cream&quot;:
 * </p>
 * 
 * <ul>
 * <li>
 * The first instance receives these words: I scream scream cream
 * </li>
 * <li>
 * The second instance receives these words: scream we for
 * </li>
 * </ul>
 *
 * <p>
 * The potential problem with this is that both instances might attempt to increment the counter for the word 
 * &quot;scream&quot; at the same time, and that may lead to a write conflict. To avoid write conflicts, we can use 
 * hash partitioning:
 *
 * <pre>
 * <code>
 * {@literal @}HashPartition("wordHash")
 * {@literal @}ProcessInput("wordOut")
 * public void process(String word) throws OperationException {
 *   this.wordCountsTable.increment(Bytes.toBytes(word), 1L);
 * }
 * </code>
 * </pre>
 * 
 * <p>
 * Now only one of the Flowlet instances will receive the word &quot;scream&quot;, and there will be no write 
 * conflicts. Note that in order to use hash partitioning, the emitting Flowlet must annotate each data object with the
 * partitioning key: 
 * </p>
 * 
 * <pre>
 * <code>
 * {@literal @}Output("wordOut")
 * private OutputEmitter{@literal <}String> wordOutput;
 *   ...
 * public void process(StreamEvent event) throws OperationException {
 *   ...
 *   // emit the word with the partitioning key “wordHash” 
 *   // (the partitioning key "wordHash" was declared in the earlier code snippet above)
 *   wordOutput.emit(word, "wordHash", word.hashCode());
 * }
 * </code>
 * </pre>
 *
 * <p>
 * Note that the emitter must use the same name ("wordHash") for the key that the consuming Flowlet specifies 
 * as the partitioning key. If the output is connected to more than one Flowlet, you can also annotate a data 
 * object with multiple hash keys – each consuming Flowlet can then use different partitioning. This is useful 
 * if you want to aggregate by multiple keys; for example, if you want to count purchases by product ID as well 
 * as by customer ID.
 * </p>
 *
 * <p>
 * Partitioning can be combined with batch execution: 
 * </p>
 *
 * <pre>
 * <code>
 * {@literal @}Batch(100)
 * {@literal @}HashPartition("wordHash")
 * {@literal @}ProcessInput("wordOut")
 * public void process(Iterator{@literal <}String> words) throws OperationException {
 *   ...
 * }
 * </code>
 * </pre>
 *
 * @see Batch
 * @see RoundRobin
 * @see ProcessInput
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface HashPartition {
  /**
   * Declare the name of the partitioning key to the process methods
   * across multiple instances of a {@link com.continuuity.api.flow.flowlet.Flowlet}.
   */
  String value();

}
