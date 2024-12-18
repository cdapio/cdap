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

package io.cdap.cdap.internal.app.runtime.distributed;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.conf.Constants.AppFabric;
import io.cdap.cdap.common.conf.Constants.Service;
import io.cdap.cdap.common.twill.AbstractMasterServiceManager;
import io.cdap.cdap.common.twill.MasterServiceManager;
import io.cdap.cdap.common.zookeeper.election.LeaderElectionInfoService;
import io.cdap.cdap.proto.Containers;
import io.cdap.cdap.proto.SystemServiceLiveInfo;
import java.net.InetAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.api.logging.LogEntry;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * App Fabric Processor Service Management in Distributed Mode.
 */
public class AppFabricProcessorManager extends AbstractMasterServiceManager {

  @Inject
  AppFabricProcessorManager(CConfiguration cConf, TwillRunner twillRunner,
      DiscoveryServiceClient discoveryClient) {
    super(cConf, discoveryClient, Constants.Service.APP_FABRIC_PROCESSOR, twillRunner);
  }

  @Override
  public String getDescription() {
    return AppFabric.PROCESSOR_DESCRIPTION;
  }
}
