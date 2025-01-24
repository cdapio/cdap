package io.cdap.cdap.internal.app.worker;


import com.google.common.net.HttpHeaders;
import com.google.gson.Gson;
import com.google.inject.Inject;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.api.messaging.TopicAlreadyExistsException;
import io.cdap.cdap.api.messaging.TopicNotFoundException;
import io.cdap.cdap.common.conf.Constants.Service;
import io.cdap.cdap.common.http.DefaultHttpRequestConfig;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.common.internal.remote.RemoteClientFactory;
import io.cdap.cdap.messaging.spi.MessageFetchRequest;
import io.cdap.cdap.messaging.spi.MessagingService;
import io.cdap.cdap.messaging.spi.MessagingServiceContext;
import io.cdap.cdap.messaging.spi.RawMessage;
import io.cdap.cdap.messaging.spi.RollbackDetail;
import io.cdap.cdap.messaging.spi.StoreRequest;
import io.cdap.cdap.messaging.spi.TopicMetadata;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.TopicId;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.common.http.HttpMethod;
import io.cdap.common.http.HttpRequest;
import io.cdap.common.http.HttpRequestConfig;
import io.cdap.common.http.HttpResponse;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DistributedMessagingService implements MessagingService {

  private static final Logger LOG = LoggerFactory.getLogger(DistributedMessagingService.class);
  private final RemoteClient remoteClient;
  private static final HttpRequestConfig HTTP_REQUEST_CONFIG = new DefaultHttpRequestConfig(false);

  private static final Gson GSON = new Gson();

  // These types for only for Gson to use, hence using the gson TypeToken instead of guava one
  @Inject
  public DistributedMessagingService(RemoteClientFactory remoteClientFactory) {
    this.remoteClient = remoteClientFactory.createRemoteClient(
        Service.APP_FABRIC_HTTP, HTTP_REQUEST_CONFIG, "/v1/namespaces/");
  }

  @Override
  public void initialize(MessagingServiceContext context) throws IOException {

  }

  @Override
  public String getName() {
    return null;
  }

  @Override
  public void createTopic(TopicMetadata topicMetadata)
      throws TopicAlreadyExistsException, IOException, UnauthorizedException {
    TopicId topicId = topicMetadata.getTopicId();
    LOG.info("sidhdirenge : DS createTopic called for {}", topicId.getTopic());
    HttpRequest request = remoteClient.requestBuilder(HttpMethod.PUT, createTopicPath(topicId))
        .withBody(GSON.toJson(topicMetadata.getProperties()))
        .build();
    HttpResponse response = remoteClient.execute(request);

    if (response.getResponseCode() == HttpURLConnection.HTTP_CONFLICT) {
      throw new TopicAlreadyExistsException(topicId.getNamespace(), topicId.getTopic());
    }
  }

  private String createTopicPath(TopicId topicId) {
    return topicId.getNamespace() + "/topics/" + topicId.getTopic();
  }

  @Override
  public void updateTopic(TopicMetadata topicMetadata)
      throws TopicNotFoundException, IOException, UnauthorizedException {

  }

  @Override
  public void deleteTopic(TopicId topicId)
      throws TopicNotFoundException, IOException, UnauthorizedException {

  }

  @Override
  public Map<String, String> getTopicMetadataProperties(TopicId topicId)
      throws TopicNotFoundException, IOException, UnauthorizedException {
    return null;
  }

  @Override
  public List<TopicId> listTopics(NamespaceId namespaceId)
      throws IOException, UnauthorizedException {
    return null;
  }

  @Nullable
  @Override
  public RollbackDetail publish(StoreRequest request)
      throws TopicNotFoundException, IOException, UnauthorizedException {
    LOG.info("sidhdirenge : DS publish called for {}", request.getTopicId().getTopic());
    Map<String, String> headers = new HashMap<>();
    headers.put(HttpHeaders.CONTENT_TYPE, "avro/binary");
    String message = "Hello";
    HttpRequest httpRequest = remoteClient.requestBuilder(HttpMethod.POST,
            createTopicPath(request.getTopicId()) + "/" + "publish")
        .addHeaders(headers)
        .withBody(message)
        .build();

    HttpResponse response = remoteClient.execute(httpRequest);

    if (response.getResponseCode() == HttpURLConnection.HTTP_NOT_FOUND) {
      throw new TopicNotFoundException(request.getTopicId().getNamespace(),
          request.getTopicId().getTopic());
    }
    return null;
  }

  @Override
  public void storePayload(StoreRequest request)
      throws TopicNotFoundException, IOException, UnauthorizedException {

  }

  @Override
  public void rollback(TopicId topicId, RollbackDetail rollbackDetail)
      throws TopicNotFoundException, IOException, UnauthorizedException {

  }

  @Override
  public CloseableIterator<RawMessage> fetch(MessageFetchRequest messageFetchRequest)
      throws TopicNotFoundException, IOException {
    return null;
  }
}
