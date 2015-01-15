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

package co.cask.cdap.data.format;

import co.cask.cdap.api.data.format.RecordFormat;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * Creates {@link RecordFormat} objects given the name of a format. Names are first checked against standard names like
 * "CSV" or "TSV". If they are not a standard name, they are interpreted as fully qualified class names.
 */
public final class RecordFormats {
  public static final String STRING = "string";
  // We may eventually want this mapping to be derived from the config.
  private static final Map<String, Class<? extends RecordFormat>> NAME_MAP =
    ImmutableMap.<String, Class<? extends RecordFormat>>builder()
      .put(STRING, SingleStringRecordFormat.class)
      .build();

  /**
   * Create a record format for the given format name. Names are first checked against standard names like "CSV" or
   * "TSV". If they are not a standard name, they are interpreted as fully qualified class names.
   *
   * @param name Name of the format to instantiate.
   * @param <FROM> Type of underlying object the format reads.
   * @param <TO> Type of object the format reads the underlying object into.
   * @return Instantiated {@link RecordFormat} based on the given name.
   * @throws IllegalAccessException if there was an illegal access when instantiating the record format.
   * @throws InstantiationException if there was an exception instantiating the record format.
   * @throws ClassNotFoundException if the record format class could not be found.
   */
  public static <FROM, TO> RecordFormat<FROM, TO> create(String name)
    throws IllegalAccessException, InstantiationException, ClassNotFoundException {
    Class<? extends RecordFormat> formatClass = NAME_MAP.get(name);
    return (RecordFormat<FROM, TO>) (formatClass == null ?
      Class.forName(name).newInstance() : formatClass.newInstance());
  }
}
