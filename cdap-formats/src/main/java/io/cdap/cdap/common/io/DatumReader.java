/*
 * Copyright © 2017 Cask Data, Inc.
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
package io.cdap.cdap.common.io;

import io.cdap.cdap.api.data.schema.Schema;
import java.io.IOException;

/**
 * Represents reader for decoding object.
 *
 * @param <T> type T to be deserialized.
 */
public interface DatumReader<T> {

  T read(Decoder decoder, Schema sourceSchema) throws IOException;
}
