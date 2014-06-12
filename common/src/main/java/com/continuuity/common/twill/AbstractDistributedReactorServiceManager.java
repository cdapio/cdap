package com.continuuity.common.twill;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.conf.Constants;
import com.google.common.base.Preconditions;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunnerService;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Abstract class that can be extended by individual Reactor Services to implement their management methods.
 */
public abstract class AbstractDistributedReactorServiceManager implements ReactorServiceManager {
  private static final long SERVICE_PING_RESPONSE_TIMEOUT = TimeUnit.MILLISECONDS.convert(5, TimeUnit.SECONDS);
  protected final long discoveryTimeout;

  protected CConfiguration cConf;
  protected TwillRunnerService twillRunnerService;
  protected String serviceName;

  public AbstractDistributedReactorServiceManager(CConfiguration cConf, String serviceName,
                                                  TwillRunnerService twillRunnerService) {
    this.cConf = cConf;
    this.serviceName = serviceName;
    this.twillRunnerService = twillRunnerService;
    this.discoveryTimeout = cConf.getLong(Constants.Monitor.DISCOVERY_TIMEOUT_SECONDS);
  }

  @Override
  public int getInstances() {
    Iterable<TwillController> twillControllerList = twillRunnerService.lookup(Constants.Service.REACTOR_SERVICES);
    int instances = 0;
    if (twillControllerList != null) {
      for (TwillController twillController : twillControllerList) {
        instances = twillController.getResourceReport().getRunnableResources(serviceName).size();
      }
    }
    return instances;
  }

  @Override
  public boolean setInstances(int instanceCount) {
    Preconditions.checkArgument(instanceCount > 0);
    try {
      Iterable<TwillController> twillControllerList = twillRunnerService.lookup(Constants.Service.REACTOR_SERVICES);
      if (twillControllerList != null) {
        for (TwillController twillController : twillControllerList) {
          twillController.changeInstances(serviceName, instanceCount).get();
        }
      }
      return true;
    } catch (InterruptedException e) {
      e.printStackTrace();
      return false;
    } catch (ExecutionException e) {
      e.printStackTrace();
      return false;
    }
  }

  @Override
  public int getMinInstances() {
    return 1;
  }

  @Override
  public boolean canCheckStatus() {
    return true;
  }

  @Override
  public boolean isLogAvailable() {
    return true;
  }

  protected HttpResponseStatus checkGetStatus(String url) throws Exception {
    try {
      URL endpoint = new URL(url);
      HttpURLConnection httpConn = (HttpURLConnection) endpoint.openConnection();
      httpConn.setConnectTimeout((int) SERVICE_PING_RESPONSE_TIMEOUT);
      return (HttpResponseStatus.valueOf(httpConn.getResponseCode()));
    } catch (SocketTimeoutException s) {
      return HttpResponseStatus.NOT_FOUND;
    }
  }

}
