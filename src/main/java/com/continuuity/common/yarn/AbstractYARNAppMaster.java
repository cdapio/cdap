package com.continuuity.common.yarn;

import com.google.common.util.concurrent.AbstractService;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.AMRMProtocol;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

/**
 *
 *
 */
public abstract class AbstractYARNAppMaster extends AbstractService {
  private static final Logger Log = LoggerFactory.getLogger(AbstractYARNAppMaster.class);
  private final YarnConfiguration yarnConfiguration;
  private final AMRMProtocol resourceMgr;
  private final Configuration conf;
  private final YarnRPC rpc;

  public AbstractYARNAppMaster(Configuration conf) {
    this.conf = conf;
    this.yarnConfiguration = new YarnConfiguration();
    rpc = YarnRPC.create(conf);

    InetSocketAddress resourceMgrAddr = NetUtils.createSocketAddr(
      yarnConfiguration.get(YarnConfiguration.RM_SCHEDULER_ADDRESS, YarnConfiguration.DEFAULT_RM_ADDRESS)
    );
    resourceMgr = (AMRMProtocol) rpc.getProxy(AMRMProtocol.class, resourceMgrAddr, conf);
  }

  /**
   *
   * @return
   */
  protected ContainerId getContainerId() {
    String containerId = System.getenv(ApplicationConstants.AM_CONTAINER_ID_ENV);
    if(containerId == null || "".equals(containerId)) {
      Log.error("Application manager container id not defined in the environment.");
      return null;
    }
    return ConverterUtils.toContainerId(containerId);
  }

  /**
   *
   * @return
   */
  private ApplicationAttemptId getAttemptId() {
    ContainerId containerId = getContainerId();
    if(containerId == null) {
      Log.error("Container Id is not defined. Please make sure AM_CONTAINER_ID is defined in the environment");
      return null;
    }
    return containerId.getApplicationAttemptId();
  }


}
