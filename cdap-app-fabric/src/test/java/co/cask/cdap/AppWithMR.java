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

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * App With MapReduce used for testing
 */
public class AppWithMR extends AbstractApplication {

  private static final Logger LOG = LoggerFactory.getLogger(WordCountApp.class);

  @Override
  public void configure() {
    setName("AppWithMR");
    setDescription("Application for testing");
    createDataset("mydataset", KeyValueTable.class);
    addMapReduce(new VoidMapReduceJob());
  }


  /**
   * Map reduce job to test MDS.
   */
  public static class VoidMapReduceJob extends AbstractMapReduce {

    @Override
    protected void configure() {
      setOutputDataset("mydataset");
      setDescription("Mapreduce that does nothing (and actually doesn't run) - it is here for testing datasets");
    }
  }
}
