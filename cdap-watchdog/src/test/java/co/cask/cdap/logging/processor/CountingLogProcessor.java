/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.logging.processor;

import ch.qos.logback.classic.spi.ILoggingEvent;
import co.cask.cdap.api.log.LogProcessor;
import com.google.common.base.Throwables;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Iterator;
import java.util.Properties;

/**
 * log processor that counts the events received - used by test case in
 * {@link co.cask.cdap.logging.save.LogSaverPluginTest}
 */
public class CountingLogProcessor implements LogProcessor {
  private int counter;
  private File outputFile;

  @Override
  public void initialize(Properties properties) {
    counter = 0;
    outputFile = new File(properties.getProperty("output.file"));
  }

  @Override
  public void process(Iterator<ILoggingEvent> events) {
    while (events.hasNext()) {
      counter++;
      events.next();
    }
  }

  @Override
  public void stop() {
    try (FileOutputStream fos = new FileOutputStream(outputFile)) {
      fos.write(counter);
      fos.flush();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
