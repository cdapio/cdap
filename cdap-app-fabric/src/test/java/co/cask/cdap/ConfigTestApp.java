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

package co.cask.cdap;

import co.cask.cdap.api.Config;
import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.worker.AbstractWorker;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class ConfigTestApp extends AbstractApplication<ConfigTestApp.ConfigClass> {
  private static final Logger LOG = LoggerFactory.getLogger(ConfigTestApp.class);

  public static final String DEFAULT_STREAM = "defaultStream";
  public static final String DEFAULT_TABLE = "defaultTable";

  public static class ConfigClass extends Config {
    private String streamName;
    private String tableName;

    public ConfigClass() {
      this.streamName = DEFAULT_STREAM;
      this.tableName = DEFAULT_TABLE;
    }

    public ConfigClass(String streamName, String tableName) {
      Preconditions.checkArgument(!Strings.isNullOrEmpty(streamName));
      Preconditions.checkArgument(!Strings.isNullOrEmpty(tableName));
      this.streamName = streamName;
      this.tableName = tableName;
    }
  }

  @Override
  public void configure() {
    ConfigClass configObj = getContext().getConfig();
    addStream(new Stream(configObj.streamName));
    createDataset(configObj.tableName, Table.class);
    addWorker(new DefaultWorker(configObj.streamName));
  }

  private static class DefaultWorker extends AbstractWorker {
    private final String streamName;
    private volatile boolean running;

    public DefaultWorker(String streamName) {
      this.streamName = streamName;
    }

    @Override
    public void run() {
      running = true;
      while (running) {
        try {
          TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
          LOG.error("Interrupted Exception", e);
        }

        try {
          getContext().write(streamName, "Hello World");
        } catch (IOException e) {
          LOG.error("IOException while trying to write to stream", e);
        }
      }
    }

    @Override
    public void stop() {
      running = false;
    }
  }
}
