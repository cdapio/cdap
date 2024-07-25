package io.cdap.cdap.ml.ext.vertexai;

import com.google.api.client.util.ExponentialBackOff;
import com.google.api.client.util.GenericData;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.io.CharStreams;
import com.google.gson.Gson;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides ComputeEngineCredentials either locally if no endpoint is provided, or remotely if
 * endpoint is provided.
 *
 * <p>This class is copied from <a href="https://github.com/cdapio/cdap"> CDAP repo </a> .
 * Copying the class keeps the dependencies for the extension simpler.
 */
public final class ComputeEngineCredentials extends GoogleCredentials {
  private static final Logger LOG = LoggerFactory.getLogger(ComputeEngineCredentials.class);
  private static final Gson GSON = new Gson();

  private static final String ACCESS_TOKEN_KEY = "access_token";
  private static final String EXPIRES_IN_KEY = "expires_in";
  private static final String LOCAL_COMPUTE_ENGINE_CREDENTIALS = "local";
  private static final ConcurrentHashMap<String, ComputeEngineCredentials> cachedComputeEngineCredentials =
      new ConcurrentHashMap<>();

  /**
   * Time (in millisecond) to refresh the credentials before it expires.
   */
  private static final int NUMBER_OF_RETRIES = 20;
  private static final int MIN_WAIT_TIME_MILLISECOND = 500;
  private static final int MAX_WAIT_TIME_MILLISECOND = 10000;
  private static final SecureRandom SECURE_RANDOM = new SecureRandom();
  private String endPoint;

  private ComputeEngineCredentials(@Nullable String endPoint) {
    this.endPoint = endPoint;
  }

  /**
   * Return a ComputeEngineCredentials with the provided endpoint if it has already been created.
   * Otherwise, it instantiates one, and returns it.
   *
   * @param endpoint endpoint for fetching the token from. A null endpoint results in fetching
   *                 the token locally.
   * @return ComputeEngineCredentials
   */
  public static ComputeEngineCredentials getOrCreate(@Nullable String endpoint) throws IOException {
    String key = endpoint != null ? endpoint : LOCAL_COMPUTE_ENGINE_CREDENTIALS;
    LOG.debug("Using token endpoint {}.", key);
    if (!cachedComputeEngineCredentials.containsKey(key)) {
      synchronized (cachedComputeEngineCredentials) {
        if (!cachedComputeEngineCredentials.containsKey(key)) {
          ComputeEngineCredentials credentials = new ComputeEngineCredentials(key);
          credentials.refresh();
          cachedComputeEngineCredentials.put(key, credentials);
        }
      }
    }

    return cachedComputeEngineCredentials.get(key);
  }

  private AccessToken getAccessTokenLocally() throws IOException {
    try {
      GoogleCredentials googleCredentials = com.google.auth.oauth2.ComputeEngineCredentials.create();
      return googleCredentials.refreshAccessToken();
    } catch (IOException e) {
      throw new IOException("Unable to get credentials from the environment. "
          + "Please explicitly set the account key.", e);
    }
  }

  private void disableVerifySsl(HttpsURLConnection connection) throws IOException {
    try {
      SSLContext sslContextWithNoVerify = SSLContext.getInstance("SSL");
      TrustManager[] trustAllCerts = new TrustManager[]{
          new X509TrustManager() {
            public X509Certificate[] getAcceptedIssuers() {
              return null;
            }

            @Override
            public void checkClientTrusted(X509Certificate[] arg0, String arg1) {
              // No-op
            }

            @Override
            public void checkServerTrusted(X509Certificate[] arg0, String arg1) {
              // No-op
            }
          }
      };
      sslContextWithNoVerify.init(null, trustAllCerts, SECURE_RANDOM);
      connection.setSSLSocketFactory(sslContextWithNoVerify.getSocketFactory());
      connection.setHostnameVerifier((s, sslSession) -> true);
    } catch (Exception e) {
      LOG.error("Unable to initialize SSL context", e);
      throw new IOException(e.getMessage());
    }
  }

  public AccessToken getAccessTokenRemotely(String endPoint) throws IOException {
    URL url = new URL(endPoint);
    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
    if (connection instanceof HttpsURLConnection) {
      // TODO (CDAP-18047) enable ssl verification
      disableVerifySsl(((HttpsURLConnection) connection));
    }
    connection.connect();
    try (Reader reader = new InputStreamReader(connection.getInputStream(),
        StandardCharsets.UTF_8)) {
      if (connection.getResponseCode() != HttpURLConnection.HTTP_OK) {
        throw new IOException(CharStreams.toString(reader));
      }
      GenericData token = GSON.fromJson(reader, GenericData.class);

      if (!token.containsKey(ACCESS_TOKEN_KEY) || !token.containsKey(EXPIRES_IN_KEY)) {
        throw new IOException("Received invalid token");
      }

      String key = token.get(ACCESS_TOKEN_KEY).toString();
      Double expiration = Double.parseDouble(token.get(EXPIRES_IN_KEY).toString());
      long expiresAtMilliseconds = System.currentTimeMillis()
          + expiration.longValue() * 1000;

      return new AccessToken(key, new Date(expiresAtMilliseconds));
    } finally {
      connection.disconnect();
    }
  }

  @Override
  public AccessToken refreshAccessToken() throws IOException {
    ExponentialBackOff backOff = new ExponentialBackOff.Builder()
        .setInitialIntervalMillis(MIN_WAIT_TIME_MILLISECOND)
        .setMaxIntervalMillis(MAX_WAIT_TIME_MILLISECOND).build();

    Exception exception = null;
    int counter = 0;
    while (counter < NUMBER_OF_RETRIES) {
      counter++;

      try {
        if (endPoint != null) {
          return getAccessTokenRemotely(endPoint);
        }
        return getAccessTokenLocally();

      } catch (Exception ex) {
        // exception does not get logged since it might get too chatty.
        exception = ex;
      }

      try {
        Thread.sleep(backOff.nextBackOffMillis());
      } catch (InterruptedException ex) {
        exception = ex;
        break;
      }
    }
    throw new IOException(exception.getMessage(), exception);
  }
}
