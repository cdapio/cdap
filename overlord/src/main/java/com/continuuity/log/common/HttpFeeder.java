package com.continuuity.log.common;

import java.io.IOException;

/**
 * Feeder through HTTP POST request.
 *
 * <p>The feeder can be configured to split all incoming texts to single lines
 * and post them
 * to the configured URL one by one. This mechanism may be required for
 * some cloud
 * logging platforms, for example for
 * <a href="http://www.loggly.com">loggly.com</a> (read their forum post about
 * <a href="http://forum.loggly.com/discussion/23">this problem</a>). You
 * enable splitting by {@code split} option set to {@code TRUE}.
 *
 * <p>The class is thread-safe.
 */
public final class HttpFeeder extends AbstractHttpFeeder {

  /**
   * End of line, for our own internal presentation.
   *
   * <p>We use this symbol in order to separate lines in buffer, not in order
   * to show them to the user. Thus, it's platform independent symbol and
   * will work on any OS (incl. Windows).
   */
  static final String EOL = "\n";

  /**
   * Shall we split lines before POST-ing?
   */
  private transient boolean split = false;

  /**
   * Set option {@code split}.
   * @param yes Shall we split?
   */
  public void setSplit(final boolean yes) {
    this.split = yes;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String toString() {
    return String.format("HTTP POST to \"%s\"", this.getUrl());
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
  public void activateOptions() {
    // empty, nothing to do here
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void feed(final String text) throws IOException {
    if (this.split) {
      for (String line : text.split(EOL)) {
        this.post(String.format("%s%s", line, EOL));
      }
    } else {
      this.post(text);
    }
  }

}