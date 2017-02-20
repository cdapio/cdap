/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.logging.framework;

import ch.qos.logback.classic.LoggerContext;
import co.cask.cdap.common.conf.CConfiguration;

import java.net.URL;

/**
 * Represents information for a log process pipeline.
 *
 * @param <T> Type of the {@link LoggerContext} object contained inside this specification
 */
public final class LogPipelineSpecification<T extends LoggerContext> {

  private final URL source;
  private final T context;
  private final CConfiguration cConf;
  private final byte[] checkpointPrefix;

  LogPipelineSpecification(URL source, T context, CConfiguration cConf, byte[] checkpointPrefix) {
    this.source = source;
    this.context = context;
    this.cConf = cConf;
    this.checkpointPrefix = checkpointPrefix;
  }

  /**
   * Returns the source URL of the log pipeline configuration that this specification was created from.
   */
  public URL getSource() {
    return source;
  }

  /**
   * Returns the name of the log processing pipeline.
   */
  public String getName() {
    return context.getName();
  }

  /**
   * Returns the {@link LoggerContext} that was created when loading the configuration.
   */
  public T getContext() {
    return context;
  }

  /**
   * Returns the {@link CConfiguration} specifically for this log processing pipeline.
   */
  public CConfiguration getConf() {
    return cConf;
  }

  /**
   * Returns the prefix to be used in the checkpoint table for this log processing pipeline.
   */
  public byte[] getCheckpointPrefix() {
    return checkpointPrefix;
  }
}
