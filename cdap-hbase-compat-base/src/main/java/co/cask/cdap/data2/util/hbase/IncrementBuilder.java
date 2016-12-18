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

package co.cask.cdap.data2.util.hbase;

import org.apache.hadoop.hbase.client.Increment;

import java.io.IOException;

/**
 * Builder for creating {@link Increment}. This builder should be used for cross HBase versions compatibility.
 * All methods on this class are just delegating to calls to {@link Increment} object.
 */
public interface IncrementBuilder {

  IncrementBuilder add(byte[] family, byte[] qualifier, long value);

  IncrementBuilder add(byte[] family, byte[] qualifier, long ts, long value) throws IOException;

  IncrementBuilder setAttribute(String name, byte[] value);

  boolean isEmpty();

  Increment build();
}
