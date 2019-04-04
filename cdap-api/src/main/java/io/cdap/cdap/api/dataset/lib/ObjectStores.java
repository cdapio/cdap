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

package io.cdap.cdap.api.dataset.lib;

import com.google.gson.Gson;
import io.cdap.cdap.api.app.ApplicationConfigurer;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.data.schema.UnsupportedTypeException;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.internal.io.ReflectionSchemaGenerator;
import io.cdap.cdap.internal.io.TypeRepresentation;

import java.lang.reflect.Type;

/**
 * Utility for describing {@link ObjectStore} and similar datasets within application configuration.
 */
public final class ObjectStores {

  private ObjectStores() {}

  /**
   * Adds an {@link ObjectStore} dataset to be created at application deploy if it does not exist.
   *
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
   *
   * @param type type of objects to be stored in dataset
   * @return {@link DatasetProperties} for the dataset
   * @throws UnsupportedTypeException
   */
  public static DatasetProperties objectStoreProperties(Type type, DatasetProperties props)
    throws UnsupportedTypeException {
    Schema schema = new ReflectionSchemaGenerator().generate(type);
    TypeRepresentation typeRep = new TypeRepresentation(type);
    return DatasetProperties.builder()
      .add("schema", schema.toString())
      .add("type", new Gson().toJson(typeRep))
      .addAll(props.getProperties())
      .build();
  }

}
