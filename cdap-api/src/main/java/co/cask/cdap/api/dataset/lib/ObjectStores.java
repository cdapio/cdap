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

package co.cask.cdap.api.dataset.lib;

import co.cask.cdap.api.app.ApplicationConfigurer;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.schema.SchemaTypeAdapter;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.internal.io.ReflectionSchemaGenerator;
import co.cask.cdap.internal.io.TypeRepresentation;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.lang.reflect.Type;

/**
 * Utility for describing {@link ObjectStore} and similar datasets within application configuration.
 */
public final class ObjectStores {
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Schema.class, new SchemaTypeAdapter())
    .create();

  private ObjectStores() {}

  /**
   * Adds {@link ObjectStore} dataset to be created at application deploy if not exists.
   * @param configurer application configurer
   * @param datasetName dataset name
   * @param type type of objects to be stored in {@link ObjectStore}
   * @param props any additional dataset properties
   * @throws UnsupportedTypeException
   */
  public static void createObjectStore(ApplicationConfigurer configurer,
                                       String datasetName, Type type, DatasetProperties props)
    throws UnsupportedTypeException {

    configurer.createDataset(datasetName, ObjectStore.class, objectStoreProperties(type, props));
  }

  /**
   * Same as {@link #createObjectStore(ApplicationConfigurer, String, Type, DatasetProperties)} but with empty
   * properties.
   */
  public static void createObjectStore(ApplicationConfigurer configurer, String datasetName, Type type)
    throws UnsupportedTypeException {

    createObjectStore(configurer, datasetName, type, DatasetProperties.EMPTY);
  }

  /**
   * Adds {@link IndexedObjectStore} dataset to be created at application deploy if not exists.
   * @param configurer application configurer
   * @param datasetName dataset name
   * @param type type of objects to be stored in {@link IndexedObjectStore}
   * @param props any additional dataset properties
   * @throws UnsupportedTypeException
   */
  public static void createIndexedObjectStore(ApplicationConfigurer configurer,
                                              String datasetName, Type type, DatasetProperties props)
    throws UnsupportedTypeException {

    configurer.createDataset(datasetName, IndexedObjectStore.class, objectStoreProperties(type, props));
  }

  /**
   * Same as {@link #createIndexedObjectStore(ApplicationConfigurer, String, Type, DatasetProperties)} but with empty
   * properties.
   */
  public static void createIndexedObjectStore(ApplicationConfigurer configurer, String datasetName, Type type)
    throws UnsupportedTypeException {

    createIndexedObjectStore(configurer, datasetName, type, DatasetProperties.EMPTY);
  }

  /**
   * Creates properties for {@link ObjectStore} dataset instance.
   * @param type type of objects to be stored in dataset
   * @return {@link DatasetProperties} for the dataset
   * @throws UnsupportedTypeException
   */
  public static DatasetProperties objectStoreProperties(Type type, DatasetProperties props)
    throws UnsupportedTypeException {
    Schema schema = new ReflectionSchemaGenerator().generate(type);
    TypeRepresentation typeRep = new TypeRepresentation(type);
    return DatasetProperties.builder()
      .add("schema", GSON.toJson(schema))
      .add("type", GSON.toJson(typeRep))
      .addAll(props.getProperties())
      .build();

  }
}
