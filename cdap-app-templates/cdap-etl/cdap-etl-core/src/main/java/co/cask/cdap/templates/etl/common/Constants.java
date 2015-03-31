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

  /**
   * Key used in the ETL Adapter Configuration which contains the Adapter Name.
   */
  public static final String ADAPTER_NAME = "name";

  public static final String CONFIG_KEY = "config";

  /**
   * Key used in the ETL Adapter Configuration which contains the schedule.
   */
  public static final String SCHEDULE_KEY = "schedule";

  /**
   * Key used in the ETL Adapter Configuration which contains the information about the source.
   */
  public static final String SOURCE_KEY = "source";

  /**
   * Key used in the ETL Adapter Configuration which contains the information about the sink.
   */
  public static final String SINK_KEY = "sink";

  /**
   * Key used in the ETL Adapter Configuration which contains the information about the transforms.
   */
  public static final String TRANSFORM_KEY = "transforms";


  public static final String PROPERTIES_KEY = "properties";

  /**
   * Constants related to Source.
   */
  public static final class Source {
    public static final String NAME = "name";
    public static final String CLASS_NAME = "templates.etl.adatper.source";
  }

  /**
   * Constants related to Sink.
   */
  public static final class Sink {
    public static final String NAME = "name";
    public static final String CLASS_NAME = "templates.etl.adapter.sink";
  }

  /**
   * Constants related to Transform.
   */
  public static final class Transform {
    public static final String NAME = "name";
    public static final String TRANSFORM_CLASS_LIST = "templates.etl.adapter.transform";
    public static final String TRANSFORM_PROPERTIES = "templates.etl.adapter.transform.properties";
  }
}
