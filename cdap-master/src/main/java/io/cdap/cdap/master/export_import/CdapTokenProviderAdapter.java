// package io.cdap.cdap.master.export_import;
//
// // The interface required by the Hadoop GCS Connector
// import com.google.cloud.hadoop.util.AccessTokenProvider;
// import org.apache.hadoop.conf.Configurable;
// import org.apache.hadoop.conf.Configuration;
// import io.cdap.cdap.runtime.spi.provisioner.dataproc.ComputeEngineCredentials;
//
// // Your custom class for fetching credentials from the endpoint
//
// import java.io.IOException;
//
// /**
//  * An adapter that uses the CDAP ComputeEngineCredentials class to provide tokens
//  * to the Hadoop GCS Connector.
//  */
// public class CdapTokenProviderAdapter implements AccessTokenProvider, Configurable {
//
//   // Define a key for a custom property to get the endpoint URL
//   public static final String TOKEN_ENDPOINT_PROPERTY = "gcs.auth.cdap.token.endpoint";
//
//   private Configuration conf;
//   private String taskWorkerEndpoint;
//
//   @Override
//   public void setConf(Configuration conf) {
//     this.conf = conf;
//     // Read the task worker endpoint URL from the Hadoop configuration
//     this.taskWorkerEndpoint = conf.get(TOKEN_ENDPOINT_PROPERTY);
//     if (taskWorkerEndpoint == null || taskWorkerEndpoint.isEmpty()) {
//       throw new IllegalArgumentException("Missing required property: " + TOKEN_ENDPOINT_PROPERTY);
//     }
//   }
//
//   @Override
//   public Configuration getConf() {
//     return this.conf;
//   }
//
//   @Override
//   public AccessToken getAccessToken() {
//     try {
//       // 1. Use your existing code to get the credentials object
//       // We'll use a max retry count of 5 as an example
//       ComputeEngineCredentials credentials = ComputeEngineCredentials.getOrCreate(taskWorkerEndpoint, 5);
//
//       // 2. Refresh the token to get the Google Auth library's AccessToken
//       com.google.auth.oauth2.AccessToken sourceToken = credentials.refreshAccessToken();
//
//       // 3. Convert from the source token type to the Hadoop connector's token type
//       String tokenString = sourceToken.getTokenValue();
//       Long expirationTimeMillis = sourceToken.getExpirationTime().getTime();
//
//       // 4. Return the token in the format the GCS connector expects
//       return new AccessToken(tokenString, expirationTimeMillis);
//
//     } catch (IOException e) {
//       throw new RuntimeException("Failed to get or refresh access token from endpoint: " + taskWorkerEndpoint, e);
//     }
//   }
//
//   @Override
//   public void refresh() {
//     // No-op. The refresh logic is already handled inside your ComputeEngineCredentials class's
//     // refreshAccessToken() method, which we call every time in getAccessToken().
//   }
// }