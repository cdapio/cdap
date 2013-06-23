package com.continuuity.log.common;

import java.io.Closeable;
import java.io.IOException;
import org.apache.log4j.spi.OptionHandler;

/**
 * Feeder of events to the cloud.
 *
 * <p>Implementation of this interface should be thread-safe.
 *
 * @author Yegor Bugayenko (yegor@rexsl.com)
 * @version $Id: Feeder.java 1401 2012-04-09 03:20:39Z guard $
 * @since 0.3.2
 */
public interface Feeder extends OptionHandler, Closeable {

  /**
   * Send this text to the cloud right now (wait as much as necessary).
   * @param text The text to send
   * @throws IOException If failed
   */
  void feed(String text) throws IOException;

}