package co.cask.cdap.gateway.router;

import co.cask.cdap.api.annotation.Service;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.internal.guava.ClassPath;
import co.cask.cdap.common.service.ServiceDiscoverable;
import co.cask.cdap.proto.ProgramType;
import co.cask.http.internal.PatternPathRouterWithGroups;
import com.google.common.annotations.VisibleForTesting;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;

public class RouterPathFinder extends HandlerInspector {

  private enum AllowedMethod {
    GET, PUT, POST, DELETE
  }

  private static final Logger LOG = LoggerFactory.getLogger(RouterAuditLookUp.class);
  private static final RouterPathFinder INSTANCE = new RouterPathFinder();
  private static final int MAX_PARTS = 25;
  private static final Map<String, RouteDestination> SERVICE_NAME_TO_ROUTER_DESTINATION = new HashMap<>();

  static {
    SERVICE_NAME_TO_ROUTER_DESTINATION.put(Constants.Service.APP_FABRIC_HTTP,
                                           new RouteDestination(Constants.Service.APP_FABRIC_HTTP));
    SERVICE_NAME_TO_ROUTER_DESTINATION.put(Constants.Service.METRICS,
                                           new RouteDestination(Constants.Service.METRICS));
    SERVICE_NAME_TO_ROUTER_DESTINATION.put(Constants.Service.DATASET_MANAGER,
                                           new RouteDestination(Constants.Service.DATASET_MANAGER));
    SERVICE_NAME_TO_ROUTER_DESTINATION.put(Constants.Service.METADATA_SERVICE,
                                           new RouteDestination(Constants.Service.METADATA_SERVICE));
    SERVICE_NAME_TO_ROUTER_DESTINATION.put(Constants.Service.EXPLORE_HTTP_USER_SERVICE,
                                           new RouteDestination(Constants.Service.EXPLORE_HTTP_USER_SERVICE));
    SERVICE_NAME_TO_ROUTER_DESTINATION.put(Constants.Service.STREAMS,
                                           new RouteDestination(Constants.Service.STREAMS));
    SERVICE_NAME_TO_ROUTER_DESTINATION.put(Constants.Service.PREVIEW_HTTP,
                                           new RouteDestination(Constants.Service.PREVIEW_HTTP));
    SERVICE_NAME_TO_ROUTER_DESTINATION.put(Constants.Service.TRANSACTION_HTTP,
                                           new RouteDestination(Constants.Service.TRANSACTION_HTTP));
    SERVICE_NAME_TO_ROUTER_DESTINATION.put(Constants.Service.TRANSACTION,
                                           new RouteDestination(Constants.Service.TRANSACTION));
    SERVICE_NAME_TO_ROUTER_DESTINATION.put(Constants.Service.LOGSAVER,
                                           new RouteDestination(Constants.Service.LOGSAVER));
    SERVICE_NAME_TO_ROUTER_DESTINATION.put(Constants.Service.METRICS_PROCESSOR,
                                           new RouteDestination(Constants.Service.METRICS_PROCESSOR));
    SERVICE_NAME_TO_ROUTER_DESTINATION.put(Constants.Service.DATASET_EXECUTOR,
                                           new RouteDestination(Constants.Service.DATASET_EXECUTOR));
    SERVICE_NAME_TO_ROUTER_DESTINATION.put(Constants.Service.MESSAGING_SERVICE,
                                           new RouteDestination(Constants.Service.MESSAGING_SERVICE));
    SERVICE_NAME_TO_ROUTER_DESTINATION.put(Constants.Router.DONT_ROUTE_SERVICE,
                                           new RouteDestination(Constants.Router.DONT_ROUTE_SERVICE));
  }

  private final int numberOfPaths;

  public static RouterPathFinder getInstance() {
    return INSTANCE;
  }

  private final PatternPathRouterWithGroups<Data> patternMatcher =
    PatternPathRouterWithGroups.create(MAX_PARTS);

  private RouterPathFinder() {
    numberOfPaths = addPathsFromHandlers();
  }

  /**
   * Returns the CDAP service which will handle the HttpRequest
   *
   * @param requestPath Normalized (and query string removed) URI path
   * @param httpRequest HttpRequest used to get the Http method and account id
   * @return destination service
   */
  @Nullable
  public RouteDestination getRoutingService(String requestPath, HttpRequest httpRequest) {
    try {
      String method = httpRequest.method().name();
      AllowedMethod requestMethod = AllowedMethod.valueOf(method);
      String[] uriParts = StringUtils.split(requestPath, '/');

      if (uriParts[0].equals(Constants.Gateway.API_VERSION_3_TOKEN)) {
        return getAuditLogContent(requestPath, new HttpMethod(requestMethod.name()));
      }
    } catch (Exception e) {
      // Ignore exception. Default routing to app-fabric.
    }
    return SERVICE_NAME_TO_ROUTER_DESTINATION.get(Constants.Service.APP_FABRIC_HTTP);
  }

  public boolean isUserServiceType(String path) {
    Pattern userServicePattern =
      Pattern.compile("/(?:v3)/(?:namespaces)/(.*?)/(?:apps)/(.*?)/(spark|services)/(.*?)/(?:methods)/(.*?)");

    Matcher matcher = userServicePattern.matcher(path);
    if (matcher.matches()) {
      for (ProgramType type : ServiceDiscoverable.getUserServiceTypes()) {
        if (type.getCategoryName().equals(matcher.group(3))) {
          return true;
        }
      }
    }
    return false;
  }


  @Nullable
  public RouteDestination getAuditLogContent(String path, HttpMethod httpMethod) {
    // Normalize the path once and strip off any query string. Just keep the URI path.
    path = URI.create(path).normalize().getPath();
    System.out.println(path);
    path = path.replaceAll("/+", "/");
    if (!path.startsWith("/")) {
      path = "/" + path;
    }
    if (isUserServiceType(path)) {
      String[] uriParts = path.split("/");
      return new RouteDestination(ServiceDiscoverable.getName(uriParts[3], uriParts[5],
                                                              ProgramType.valueOfCategoryName(uriParts[6]),
                                                              uriParts[7]));
//      return SERVICE_NAME_TO_ROUTER_DESTINATION.get(Constants.Service.APP_FABRIC_HTTP);
    }
    List<PatternPathRouterWithGroups.RoutableDestination<Data>> destinations =
      patternMatcher.getDestinations(path);
    for (PatternPathRouterWithGroups.RoutableDestination<Data> entry : destinations) {
      Data destination = entry.getDestination();
      if (destination.httpMethod.equals(httpMethod)) {
        if (destination.service.value().equalsIgnoreCase(Constants.Router.DYNAMIC_ROUTE_SERVICE)) {
          String actualServiceName = path.split("/")[destination.service.position() + 1];
          return SERVICE_NAME_TO_ROUTER_DESTINATION.get(actualServiceName);
        }
        return SERVICE_NAME_TO_ROUTER_DESTINATION.get(destination.service.value());
      }
    }
    return null;
  }

  private int addPathsFromHandlers() {
    List<ClassPath.ClassInfo> handlerClasses;
    try {
      handlerClasses = getAllHandlerClasses();
    } catch (IOException e) {
      LOG.error("Failed to get all handler classes for audit logging: {}", e.getCause());
      return -1;
    }

    int count = 0;
    for (ClassPath.ClassInfo classInfo : handlerClasses) {
      Class<?> handlerClass = classInfo.load();

      Service handlerName = handlerClass.getAnnotation(Service.class);
      Path classPath = handlerClass.getAnnotation(Path.class);
      String classPathStr = classPath == null ? "" : classPath.value();
      for (java.lang.reflect.Method method : handlerClass.getMethods()) {
        Path methodPath = method.getAnnotation(Path.class);
        Service methodService = method.getAnnotation(Service.class);
        HttpMethod httpMethod = getHttpMethod(method);
        if (methodPath == null || httpMethod == null || handlerName == null) {
          continue;
        }
        if (methodPath.value().contains("query")) {
          Annotation[][] annotations = method.getParameterAnnotations();
          for (Annotation[] ann : annotations) {
            for (Annotation annotation : ann) {
              if (annotation instanceof QueryParam) {
                System.out.println("####");
                System.out.println(((QueryParam) annotation).value());
              }
            }
          }
        }
        // if a service annotation was defined on a method that take precedence over method
        if (methodService != null) {
          handlerName = methodService;
        }

        String methodPathStr = methodPath.value();
        String completePath = classPathStr.endsWith("/") || methodPathStr.startsWith("/")
          ? classPathStr + methodPathStr : classPathStr + "/" + methodPathStr;
        patternMatcher.add(completePath, new Data(httpMethod, handlerName));
        // Don't count classes in unit-tests
        if (!isTestClass(classInfo)) {
          count++;
        }
      }
    }
    LOG.info("Router lookup: bootstrapped with {} paths", count);
    return count;
  }

  class Data {
    HttpMethod httpMethod;
    Service service;

    public Data(HttpMethod httpMethod, Service service) {
      this.httpMethod = httpMethod;
      this.service = service;
    }
  }


  @VisibleForTesting
  int getNumberOfPaths() {
    return numberOfPaths;
  }
}