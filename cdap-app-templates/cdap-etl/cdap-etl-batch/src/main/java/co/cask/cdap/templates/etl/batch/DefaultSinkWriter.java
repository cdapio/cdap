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

package co.cask.cdap.templates.etl.batch;

import co.cask.cdap.templates.etl.api.batch.SinkWriter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * SinkWriter that writes key value pairs as output of a Mapper.
 *
 * @param <KEY> the type of key to write
 * @param <VAL> the type of value to write
 */
public class DefaultSinkWriter<KEY, VAL> implements SinkWriter<KEY, VAL> {
  private final Mapper.Context context;

  public DefaultSinkWriter(Mapper.Context context) {
    this.context = context;
  }

  @Override
  public void write(KEY key, VAL val) throws IOException, InterruptedException {
    context.write(key, val);
  }
}
