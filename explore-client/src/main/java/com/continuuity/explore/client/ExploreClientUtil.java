package com.continuuity.explore.client;

import com.continuuity.explore.service.Explore;
import com.continuuity.explore.service.ExploreException;
import com.continuuity.explore.service.Handle;
import com.continuuity.explore.service.HandleNotFoundException;
import com.continuuity.explore.service.Status;
import com.google.common.base.Charsets;
import com.google.common.base.Objects;
import com.google.gson.JsonSyntaxException;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.handler.codec.http.HttpRequest;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Helper methods for explore client.
 */
public class ExploreClientUtil {

  /**
   * Polls for state of the operation represented by the {@link Handle}, and returns when operation has completed
   * execution.
   * @param exploreClient explore client used to poll status.
   * @param handle handle representing the operation.
   * @param sleepTime time to sleep between pooling.
   * @param timeUnit unit of sleepTime.
   * @param maxTries max number of times to poll.
   * @return completion status of the operation, null on reaching maxTries.
   * @throws ExploreException
   * @throws HandleNotFoundException
   * @throws InterruptedException
   */
  public static Status waitForCompletionStatus(Explore exploreClient, Handle handle,
                                               long sleepTime, TimeUnit timeUnit, int maxTries)
    throws ExploreException, HandleNotFoundException, InterruptedException, SQLException {
    Status status;
    int tries = 0;
    do {
      timeUnit.sleep(sleepTime);
      status = exploreClient.getStatus(handle);

      if (++tries > maxTries) {
        break;
      }
    } while (status.getStatus() == Status.OpStatus.RUNNING || status.getStatus() == Status.OpStatus.PENDING ||
             status.getStatus() == Status.OpStatus.INITIALIZED || status.getStatus() == Status.OpStatus.UNKNOWN);
    return status;
  }

  /**
   * Class to represent a JSON object passed as an argument to the getTables metadata HTTP endpoint.
   */
  public static final class TablesArgs {
    private final String catalogName;
    private final String schemaNamePattern;
    private final String tableNamePattern;
    private final List<String> tableTypes;

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
        .add("catalogName", catalogName)
        .add("schemaNamePattern", schemaNamePattern)
        .add("tableNamePattern", tableNamePattern)
        .add("tableTypes", tableTypes)
        .toString();
    }

    public TablesArgs(String catalogName, String schemaNamePattern, String tableNamePattern, List<String> tableTypes) {
      this.catalogName = catalogName;
      this.schemaNamePattern = schemaNamePattern;
      this.tableNamePattern = tableNamePattern;
      this.tableTypes = tableTypes;
    }

    public String getTableNamePattern() {
      return tableNamePattern;
    }

    public List<String> getTableTypes() {
      return tableTypes;
    }

    public String getSchemaNamePattern() {
      return schemaNamePattern;
    }

    public String getCatalogName() {
      return catalogName;
    }
  }
}
