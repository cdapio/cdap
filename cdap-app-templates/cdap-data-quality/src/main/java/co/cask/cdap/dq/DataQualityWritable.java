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

package co.cask.cdap.dq;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.GenericWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * Class for Data Quality Writable. To be used as output in mapper in DataQualityApp
 */
public class DataQualityWritable extends GenericWritable {
  private static final Class [] CLASSES = {
    Text.class,
    LongWritable.class,
    DoubleWritable.class,
    IntWritable.class,
    Float.class,
    Boolean.class,
    Byte.class
  };

  @Override
  protected Class<? extends Writable>[] getTypes() {
    return CLASSES;
  }
}
