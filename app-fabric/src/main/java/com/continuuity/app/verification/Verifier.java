package com.continuuity.app.verification;

/**
 * A verifier for verifying the specifications provided.
 * <p>
 * Implementors of this interface will take Specification as input
 * and verify the information within the specification meets satisfies
 * all the checkpoints.
 * </p>
 * <p/>
 * <p>
 * Implementation of this interface are expected to be thread-safe,
 * an can be safely accessed by multiple concurrent threads.
 * </p>
 *
 * @param <T> Type of object to be verified.
 */
public interface Verifier<T> {

  /**
   * Verifies <code>input</code> and returns {@link VerifyResult}
   * containing the status of verification.
   *
   * @param input to be verified
   * @return An instance of {@link VerifyResult}
   */
  VerifyResult verify(T input);
}
