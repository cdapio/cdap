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

package co.cask.cdap.templates.etl.lib.sources.batch;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.mapreduce.MapReduceContext;
import co.cask.cdap.templates.etl.api.StageConfigurer;
import co.cask.cdap.templates.etl.api.batch.BatchSource;
import co.cask.cdap.templates.etl.api.realtime.Emitter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class KVBatchSource implements BatchSource<byte[], byte[], String> {
  private static final Logger LOG = LoggerFactory.getLogger(KVBatchSource.class);

  @Override
  public void configure(StageConfigurer configurer) {
    configurer.setName("KVBatchSource");
    configurer.setDescription("Key Value Table Batch Source");
  }

  @Override
  public void prepareJob(MapReduceContext context) {
    context.setInput("KVTableBatchSource");
  }

  @Override
  public void initialize(MapReduceContext context) {
    //no-op
  }

  @Override
  public void read(byte[] key, byte[] value, Emitter<String> data) {
    data.emit(Bytes.toString(key));
  }

  @Override
  public void onFinish(boolean succeeded, MapReduceContext context) throws Exception {
    LOG.info("MR Source complete : {}", succeeded);
  }

  @Override
  public void destroy() {
    //no-op
  }
}
