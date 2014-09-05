/*
 * Copyright 2014 Cask Data, Inc.
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

package co.cask.cdap.examples.sparkkmeans;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.lib.ObjectStores;
import co.cask.cdap.api.spark.AbstractSpark;
import co.cask.cdap.api.spark.SparkSpecification;
import co.cask.cdap.internal.io.UnsupportedTypeException;

import java.nio.charset.Charset;

/**
 * Created by dev on 9/5/14.
 */
public class SparkKMeansApp extends AbstractApplication {

  public static final Charset UTF8 = Charset.forName("UTF-8");

  @Override
  public void configure() {
    setName("SparkKMeans");
    setDescription("Spark KMeans app");
    addStream(new Stream("pointsStream"));
    addFlow(new PointsFlow());
    addSpark(new SparkKMeansJob());
    addProcedure(new CentersProcedure());

    try {
      ObjectStores.createObjectStore(getConfigurer(), "points", String.class);
      ObjectStores.createObjectStore(getConfigurer(), "centers", Double.class);
    } catch (UnsupportedTypeException e) {
      // This exception is thrown by ObjectStore if its parameter type cannot be
      // (de)serialized (for example, if it is an interface and not a class, then there is
      // no auto-magic way deserialize an object.) In this case that will not happen
      // because String are actual classes.
      throw new RuntimeException(e);
    }
  }

  /**
   *
   */
  public static class SparkKMeansJob extends AbstractSpark {
    @Override
    public SparkSpecification configure() {
      return SparkSpecification.Builder.with()
        .setName("SparkKMeansJob")
        .setDescription("Spark KMeans Job")
        .setMainClassName("co.cask.cdap.examples.sparkkmeans.SparkKMeansJobBuilder")
        .build();
    }
  }
}
