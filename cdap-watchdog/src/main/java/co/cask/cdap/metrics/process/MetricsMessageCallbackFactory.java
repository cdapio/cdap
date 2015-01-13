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
package co.cask.cdap.metrics.process;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.common.metrics.MetricsScope;
import co.cask.cdap.internal.io.DatumReader;
import co.cask.cdap.internal.io.DatumReaderFactory;
import co.cask.cdap.internal.io.SchemaGenerator;
import co.cask.cdap.metrics.MetricsConstants;
import co.cask.cdap.metrics.transport.MetricValue;
import com.google.common.base.Throwables;
import com.google.common.reflect.TypeToken;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.twill.kafka.client.KafkaConsumer;

import java.util.Set;

/**
 * A {@link MessageCallbackFactory} that creates MessageCallback for processing MetricsRecord
 * with offset persists to {@link KafkaConsumerMetaTable}.
 */
public final class MetricsMessageCallbackFactory implements MessageCallbackFactory {

  private final DatumReader<MetricValue> datumReader;
  private final Schema recordSchema;
  private final Set<MetricsProcessor> processors;
  private final int persistThreshold;

  @Inject
  public MetricsMessageCallbackFactory(SchemaGenerator schemaGenerator, DatumReaderFactory readerFactory,
                                       Set<MetricsProcessor> processors,
                                       @Named(MetricsConstants.ConfigKeys.KAFKA_CONSUMER_PERSIST_THRESHOLD)
                                       int persistThreshold) {
    try {
      this.recordSchema = schemaGenerator.generate(MetricValue.class);
      this.datumReader = readerFactory.create(TypeToken.of(MetricValue.class), recordSchema);
      this.processors = processors;
      this.persistThreshold = persistThreshold;

    } catch (UnsupportedTypeException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public KafkaConsumer.MessageCallback create(KafkaConsumerMetaTable metaTable, MetricsScope scope) {
    return new PersistedMessageCallback(
      new MetricsMessageCallback(scope, processors, datumReader, recordSchema), metaTable, persistThreshold);
  }
}
