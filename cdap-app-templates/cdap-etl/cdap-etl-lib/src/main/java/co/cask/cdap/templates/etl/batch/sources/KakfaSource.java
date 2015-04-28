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

package co.cask.cdap.templates.etl.batch.sources;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.templates.etl.api.Emitter;
import co.cask.cdap.templates.etl.api.batch.BatchSource;
import co.cask.cdap.templates.etl.api.batch.BatchSourceContext;
import co.cask.cdap.templates.etl.common.kafka.CamusWrapper;
import co.cask.cdap.templates.etl.common.kafka.EtlInputFormat;
import co.cask.cdap.templates.etl.common.kafka.EtlKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

/**
 *
 */
@Plugin(type = "source")
@Name("Kafka")
@Description("Batch Source for Kafka")
public class KakfaSource extends BatchSource<EtlKey, CamusWrapper, StructuredRecord> {

  @Override
  public void prepareJob(BatchSourceContext context) {
    Job job = context.getHadoopJob();
    Configuration conf = job.getConfiguration();
    job.setInputFormatClass(EtlInputFormat.class);
  }

  @Override
  public void transform(KeyValue<EtlKey, CamusWrapper> input, Emitter<StructuredRecord> emitter) throws Exception {

  }
}
