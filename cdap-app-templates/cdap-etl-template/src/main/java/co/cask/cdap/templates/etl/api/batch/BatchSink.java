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

package co.cask.cdap.templates.etl.api.batch;

import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.templates.etl.api.StageConfigurer;
import co.cask.cdap.templates.etl.api.StageLifecycle;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Batch Sink
 */
public interface BatchSink<I> extends StageLifecycle {

  void configure(StageConfigurer configurer);

  void prepareJob(MapReduceContext context);

  void initialize(MapReduceContext context);

  void write(Mapper.Context context, I input) throws IOException, InterruptedException;

  void onFinish(boolean succeeded, MapReduceContext context);
}
