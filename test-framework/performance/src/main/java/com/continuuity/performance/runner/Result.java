package com.continuuity.performance.runner;

import org.junit.runner.notification.Failure;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A <code>Result</code> collects and summarizes information from running multiple tests.
 * All tests are counted -- additional information is collected from tests that fail.
 *
 * @since 4.0
 */
public class Result implements Serializable {
  private static final long serialVersionUID = 1L;
  private AtomicInteger fCount = new AtomicInteger();
  private AtomicInteger fIgnoreCount = new AtomicInteger();
  private final List<Failure> fFailures = Collections.synchronizedList(new ArrayList<Failure>());
  private long fRunTime = 0;
  private long fStartTime;

  /**
   * @return the number of tests run
   */
  public int getRunCount() {
    return fCount.get();
  }

  /**
   * @return the number of tests that failed during the run
   */
  public int getFailureCount() {
    return fFailures.size();
  }

  /**
   * @return the number of milliseconds it took to run the entire suite to run
   */
  public long getRunTime() {
    return fRunTime;
  }

  /**
   * @return the {@link Failure}s describing tests that failed and the problems they encountered
   */
  public List<Failure> getFailures() {
    return fFailures;
  }

  /**
   * @return the number of tests ignored during the run
   */
  public int getIgnoreCount() {
    return fIgnoreCount.get();
  }

  /**
   * @return <code>true</code> if all tests succeeded
   */
  public boolean wasSuccessful() {
    return getFailureCount() == 0;
  }
}
