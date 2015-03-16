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

package co.cask.cdap.explore.service;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.Marker;

/**
 *
 */
public class HiveStreamRedirectorTest {

  @Test
  public void testLoggerRedirector() throws Exception {
    CountingLogger logger = new CountingLogger();
    SessionState sessionState = new SessionState(new HiveConf());

    HiveStreamRedirector.redirectToLogger(sessionState, logger);

    for (int i = 0; i < 5; i++) {
      sessionState.out.print("testInfoLog" + i);
    }

    Assert.assertEquals(5, logger.getInfoLogs());
  }

  /**
   * A logger that increments counters for simple info logs.
   */
  private static final class CountingLogger implements Logger {
    private int infoLogs = 0;

    public int getInfoLogs() {
      return infoLogs;
    }

    @Override
    public String getName() {
      return "CountingLogger";
    }

    @Override
    public boolean isTraceEnabled() {
      return false;
    }

    @Override
    public void trace(String msg) {
      // no-op
    }

    @Override
    public void trace(String format, Object arg) {
      // no-op
    }

    @Override
    public void trace(String format, Object arg1, Object arg2) {
      // no-op
    }

    @Override
    public void trace(String format, Object... arguments) {
      // no-op
    }

    @Override
    public void trace(String msg, Throwable t) {
      // no-op
    }

    @Override
    public boolean isTraceEnabled(Marker marker) {
      return false;
    }

    @Override
    public void trace(Marker marker, String msg) {
      // no-op
    }

    @Override
    public void trace(Marker marker, String format, Object arg) {
      // no-op
    }

    @Override
    public void trace(Marker marker, String format, Object arg1, Object arg2) {
      // no-op
    }

    @Override
    public void trace(Marker marker, String format, Object... argArray) {
      // no-op
    }

    @Override
    public void trace(Marker marker, String msg, Throwable t) {
      // no-op
    }

    @Override
    public boolean isDebugEnabled() {
      return false;
    }

    @Override
    public void debug(String msg) {
      // no-op
    }

    @Override
    public void debug(String format, Object arg) {
      // no-op
    }

    @Override
    public void debug(String format, Object arg1, Object arg2) {
      // no-op
    }

    @Override
    public void debug(String format, Object... arguments) {
      // no-op
    }

    @Override
    public void debug(String msg, Throwable t) {
      // no-op
    }

    @Override
    public boolean isDebugEnabled(Marker marker) {
      return false;
    }

    @Override
    public void debug(Marker marker, String msg) {
      // no-op
    }

    @Override
    public void debug(Marker marker, String format, Object arg) {
      // no-op
    }

    @Override
    public void debug(Marker marker, String format, Object arg1, Object arg2) {
      // no-op
    }

    @Override
    public void debug(Marker marker, String format, Object... arguments) {
      // no-op
    }

    @Override
    public void debug(Marker marker, String msg, Throwable t) {
      // no-op
    }

    @Override
    public boolean isInfoEnabled() {
      return true;
    }

    @Override
    public void info(String msg) {
      infoLogs++;
    }

    @Override
    public void info(String format, Object arg) {
      // no-op
    }

    @Override
    public void info(String format, Object arg1, Object arg2) {
      // no-op
    }

    @Override
    public void info(String format, Object... arguments) {
      // no-op
    }

    @Override
    public void info(String msg, Throwable t) {
      // no-op
    }

    @Override
    public boolean isInfoEnabled(Marker marker) {
      return false;
    }

    @Override
    public void info(Marker marker, String msg) {
      // no-op
    }

    @Override
    public void info(Marker marker, String format, Object arg) {
      // no-op
    }

    @Override
    public void info(Marker marker, String format, Object arg1, Object arg2) {
      // no-op
    }

    @Override
    public void info(Marker marker, String format, Object... arguments) {
      // no-op
    }

    @Override
    public void info(Marker marker, String msg, Throwable t) {
      // no-op
    }

    @Override
    public boolean isWarnEnabled() {
      return false;
    }

    @Override
    public void warn(String msg) {
      // no-op
    }

    @Override
    public void warn(String format, Object arg) {
      // no-op
    }

    @Override
    public void warn(String format, Object... arguments) {
      // no-op
    }

    @Override
    public void warn(String format, Object arg1, Object arg2) {
      // no-op
    }

    @Override
    public void warn(String msg, Throwable t) {
      // no-op
    }

    @Override
    public boolean isWarnEnabled(Marker marker) {
      return false;
    }

    @Override
    public void warn(Marker marker, String msg) {
      // no-op
    }

    @Override
    public void warn(Marker marker, String format, Object arg) {
      // no-op
    }

    @Override
    public void warn(Marker marker, String format, Object arg1, Object arg2) {
      // no-op
    }

    @Override
    public void warn(Marker marker, String format, Object... arguments) {
      // no-op
    }

    @Override
    public void warn(Marker marker, String msg, Throwable t) {
      // no-op
    }

    @Override
    public boolean isErrorEnabled() {
      return false;
    }

    @Override
    public void error(String msg) {
      // no-op
    }

    @Override
    public void error(String format, Object arg) {
      // no-op
    }

    @Override
    public void error(String format, Object arg1, Object arg2) {
      // no-op
    }

    @Override
    public void error(String format, Object... arguments) {
      // no-op
    }

    @Override
    public void error(String msg, Throwable t) {
      // no-op
    }

    @Override
    public boolean isErrorEnabled(Marker marker) {
      return false;
    }

    @Override
    public void error(Marker marker, String msg) {
      // no-op
    }

    @Override
    public void error(Marker marker, String format, Object arg) {
      // no-op
    }

    @Override
    public void error(Marker marker, String format, Object arg1, Object arg2) {
      // no-op
    }

    @Override
    public void error(Marker marker, String format, Object... arguments) {
      // no-op
    }

    @Override
    public void error(Marker marker, String msg, Throwable t) {
      // no-op
    }
  }

}
