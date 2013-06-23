package com.continuuity.log.common;

/**
 * Feeder to console.
 * <p>The class is thread-safe.
 */
public final class ConsoleFeeder implements Feeder {

  /**
   * {@inheritDoc}
   */
  @Override
  public String toString() {
    return "Console Feeder";
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void close() {
    // nothing to close here
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void feed(final String text) {
    System.out.print(text);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void activateOptions() {
    // intentionally empty
  }

}