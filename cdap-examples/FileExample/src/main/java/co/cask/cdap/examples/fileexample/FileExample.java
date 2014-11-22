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

package co.cask.cdap.examples.fileexample;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import com.google.common.base.Throwables;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * An application that uses files for its MapReduce input and output.
 */
public class FileExample extends AbstractApplication {

  @Override
  public void configure() {
    try {
      setName("FileExample");
      setDescription("Application with MapReduce job using file as dataset");
      createDataset("lines", "fileSet", FileSetProperties.builder()
        .setInputFormat(TextInputFormat.class)
        .setOutputFormat(TextOutputFormat.class).build());
      createDataset("counts", "fileSet", FileSetProperties.builder()
          .setInputFormat(TextInputFormat.class)
          .setOutputFormat(TextOutputFormat.class).build());
      addService(new FileService());
      addMapReduce(new WordCount());
    } catch (Throwable t) {
      throw Throwables.propagate(t);
    }
  }
}
