/*
 * Copyright © 2014-2015 Cask Data, Inc.
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
package co.cask.cdap.data.stream;

import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.data.file.FileWriter;
import co.cask.cdap.data2.transaction.stream.StreamConfig;

import java.io.IOException;

/**
 *
 */
public interface StreamFileWriterFactory {

  /**
   * Returns the file name prefix of all stream files created through this factory.
   */
  String getFileNamePrefix();

  FileWriter<StreamEvent> create(StreamConfig config, int generation) throws IOException;
}
