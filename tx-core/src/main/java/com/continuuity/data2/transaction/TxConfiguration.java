/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.data2.transaction;

import org.apache.hadoop.conf.Configuration;

/**
 *  Utility Class to get the Configuration with tx-default and tx-site configurations added to it.
 */
public class TxConfiguration {

  public static Configuration getConfiguration() {
    Configuration conf = new Configuration();
    conf.addResource("tx-default.xml");
    conf.addResource("tx-site.xml");
    return conf;
  }

}
