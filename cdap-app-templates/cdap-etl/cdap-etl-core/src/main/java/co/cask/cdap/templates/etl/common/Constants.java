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

package co.cask.cdap.templates.etl.common;

/**
 * Constants used in ETL Adapter.
 */
public final class Constants {

  public static final String ADAPTER_NAME = "name";
  public static final String CONFIG_KEY = "config";

  /**
   * Constants related to Source.
   */
  public static final class Source {
    public static final String SPECIFICATION = "templates.etl.adapter.source.specification";
  }

  /**
   * Constants related to Sink.
   */
  public static final class Sink {
    public static final String SPECIFICATION = "templates.etl.adapter.sink.specification";
  }

  /**
   * Constants related to Transform.
   */
  public static final class Transform {
    public static final String SPECIFICATIONS = "templates.etl.adapter.transform.specifications";
  }
}
