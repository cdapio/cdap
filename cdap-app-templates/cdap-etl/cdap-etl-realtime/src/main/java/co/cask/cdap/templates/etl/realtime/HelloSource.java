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

package co.cask.cdap.templates.etl.realtime;

import co.cask.cdap.templates.etl.api.Emitter;
import co.cask.cdap.templates.etl.api.StageConfigurer;
import co.cask.cdap.templates.etl.api.realtime.RealtimeSource;
import co.cask.cdap.templates.etl.api.realtime.SourceState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 *
 */
public class HelloSource extends RealtimeSource<String> {
  private static final Logger LOG = LoggerFactory.getLogger(HelloSource.class);

  @Override
  public void configure(StageConfigurer configurer) {
    configurer.setName(HelloSource.class.getSimpleName());
  }

  @Nullable
  @Override
  public SourceState poll(Emitter<String> writer, SourceState currentState) {
    try {
      TimeUnit.SECONDS.sleep(1);
    } catch (InterruptedException e) {
      LOG.error("Some Error in Source");
    }
    LOG.info("Emitting data!");
    writer.emit("Hello");
    return null;
  }
}
