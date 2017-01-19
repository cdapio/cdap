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

package co.cask.cdap.api.dataset;

import java.util.Map;
import javax.annotation.Nullable;

/**
 * Useful class to configure dataset properties for Explore.
 */
@SuppressWarnings({"unused", "WeakerAccess"})
public class ExploreProperties {

  /**
   * Property to configure the name of the Explore table for a dataset; that is, the table in the external
   * system that implements Explore, such as Hive.
   */
  public static final String PROPERTY_EXPLORE_TABLE_NAME = "explore.table.name";

  /**
   * Property to configure the name of the Explore database for a dataset; that is, the database in the external
   * system that implements Explore, such as Hive.
   */
  public static final String PROPERTY_EXPLORE_DATABASE_NAME = "explore.database.name";

  /**
   * @return the name of the Explore table configured by the properties, or null if not configured.
   */
  @Nullable
  public static String getExploreTableName(DatasetProperties props) {
    return getExploreTableName(props.getProperties());
  }

  /**
   * @return the name of the Explore table configured by the properties, or null if not configured.
   */
  @Nullable
  public static String getExploreTableName(Map<String, String> props) {
    return props.get(PROPERTY_EXPLORE_TABLE_NAME);
  }

  /**
   * @return the name of the Explore database configured by the properties, or null if not configured.
   */
  @Nullable
  public static String getExploreDatabaseName(DatasetProperties props) {
    return getExploreDatabaseName(props.getProperties());
  }

  /**
   * @return the name of the Explore database configured by the properties, or null if not configured.
   */
  @Nullable
  public static String getExploreDatabaseName(Map<String, String> props) {
    return props.get(PROPERTY_EXPLORE_DATABASE_NAME);
  }

  /**
   * Set the name of the Explore table to be used for Explore.
   */
  public static void setExploreTableName(DatasetProperties.Builder builder, String tableName) {
    builder.add(PROPERTY_EXPLORE_TABLE_NAME, tableName);
  }

  /**
   * Set the name of the Explore database to be used for Explore.
   */
  public static void setExploreDatabaseName(DatasetProperties.Builder builder, String databaseName) {
    builder.add(PROPERTY_EXPLORE_DATABASE_NAME, databaseName);
  }

  public static Builder builder() {
    return new Builder();
  }

  /**
   * A builder for dataset properties for Explore.
   */
  public static class Builder extends AbstractBuilder<Builder> { }

  /**
   * A builder for dataset properties for Explore.
   *
   * @param <B> the type of the builder
   */
  public abstract static class AbstractBuilder<B extends AbstractBuilder> extends DatasetProperties.Builder {

    /**
     * Set the name of the Explore table to be used for Explore.
     */
    @SuppressWarnings("unchecked")
    public B setExploreTableName(String tableName) {
      ExploreProperties.setExploreTableName(this, tableName);
      return (B) this;
    }

    /**
     * Set the name of the Explore database to be used for Explore.
     */
    @SuppressWarnings("unchecked")
    public B setExploreDatabaseName(String databaseName) {
      ExploreProperties.setExploreDatabaseName(this, databaseName);
      return (B) this;
    }
  }
}
