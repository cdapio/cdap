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

package $package;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.mapreduce.AbstractMapReduce;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import org.apache.hadoop.mapreduce.Job;

/**
 * This is a simple application that includes a MapReduce program.
 */
public class MapReduceApp extends AbstractApplication {

  @Override
  public void configure() {
    setName("MapReduceApp");
    setDescription("A simple application that includes a MapReduce program");
    addMapReduce(new MyMapReduce());
  }

  /**
   * Sample MapReduce.
   */
  public static final class MyMapReduce extends AbstractMapReduce {

    @Override
    public void configure() {
      // TODO set name, description etc.
    }

    @Override
    public void beforeSubmit(MapReduceContext context) throws Exception {
      // Get the Hadoop job context
      Job job = (Job) context.getHadoopJob();

      // TODO: set Mapper, reducer, combiner, etc.
    }

    @Override
    public void onFinish(boolean success, MapReduceContext context) throws Exception {
      // TODO: whatever is necessary after job finishes
    }
  }
}
