package co.cask.cdap.gateway.handlers;

import co.cask.cdap.app.store.ServiceStore;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.twill.MasterServiceManager;
import co.cask.cdap.gateway.auth.Authenticator;
import co.cask.http.HttpResponder;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpRequest;

import java.util.Map;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 *
 */
@Path(Constants.Gateway.API_VERSION_2)
public class MonitorHandlerV2 extends AbstractMonitorHandler {
  @Inject
  public MonitorHandlerV2(Authenticator authenticator, Map<String, MasterServiceManager> serviceMap,
                          ServiceStore serviceStore) throws Exception {
    super(authenticator, serviceMap, serviceStore);
  }

  /**
   * Returns the number of instances of CDAP Services
   */
  @Path("/system/services/{service-name}/instances")
  @GET
  public void getServiceInstance(final HttpRequest request, final HttpResponder responder,
                                 @PathParam("service-name") String serviceName) throws Exception {

    super.getServiceInstance(request, responder, serviceName);
  }

  /**
   * Sets the number of instances of CDAP Services
   */
  @Path("/system/services/{service-name}/instances")
  @PUT
  public void setServiceInstance(final HttpRequest request, final HttpResponder responder,
                                 @PathParam("service-name") final String serviceName) {
    super.setServiceInstance(request, responder, serviceName);
  }

  // Return the status of CDAP services in JSON format
  @Path("/system/services/status")
  @GET
  public void getBootStatus(final HttpRequest request, final HttpResponder responder) {
    super.getBootStatus(request, responder);
  }

  @Path("/system/services/{service-name}/status")
  @GET
  public void monitor(final HttpRequest request, final HttpResponder responder,
                      @PathParam("service-name") final String serviceName) {
    super.monitor(request, responder, serviceName);
  }

  @Path("/system/services")
  @GET
  public void getServiceSpec(final HttpRequest request, final HttpResponder responder) throws Exception {
    super.getServiceSpec(request, responder);
  }
}
