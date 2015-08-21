/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.examples.fileset;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * An application that illustrates how to use the FileSet dataset.
 * <ul>
 * <li>Uses a Service that uploads files in—or downloads files to—a FileSet.</li>
 * <li>Includes a MapReduce that implements the classic word count example. The input and output paths
 *     of the MapReduce can be configured through runtime arguments.</li>
 * </ul>
 */
public class FileSetExample extends AbstractApplication {

  @Override
  public void configure() {
    setName("FileSetExample");
    setDescription("Application with a MapReduce that uses a FileSet dataset");
    createDataset("lines", FileSet.class, FileSetProperties.builder()
      .setBasePath("example/data/lines")
      .setInputFormat(TextInputFormat.class)
      .setOutputFormat(TextOutputFormat.class)
      .build());
    createDataset("counts", FileSet.class, FileSetProperties.builder()
      .setInputFormat(TextInputFormat.class)
      .setOutputFormat(TextOutputFormat.class)
      .setOutputProperty(TextOutputFormat.SEPERATOR, ":")
      .build());
    addService(new FileSetService());
    addMapReduce(new WordCount());
  }
}
