/*
 * Copyright © 2012-2014 Cask Data, Inc.
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

package co.cask.cdap.cli.command;

import co.cask.cdap.cli.ArgumentName;
import co.cask.cdap.cli.CLIConfig;
import co.cask.cdap.cli.Categorized;
import co.cask.cdap.cli.CommandCategory;
import co.cask.cdap.cli.ElementType;
import co.cask.cdap.cli.english.Article;
import co.cask.cdap.cli.english.Fragment;
import co.cask.cdap.cli.util.AbstractAuthCommand;
import co.cask.cdap.cli.util.RowMaker;
import co.cask.cdap.cli.util.table.Table;
import co.cask.cdap.client.QueryClient;
import co.cask.cdap.explore.client.ExploreExecutionResult;
import co.cask.cdap.explore.service.HandleNotFoundException;
import co.cask.cdap.proto.ColumnDesc;
import co.cask.cdap.proto.QueryResult;
import co.cask.cdap.proto.QueryStatus;
import co.cask.common.cli.Arguments;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;

import java.io.PrintStream;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Executes a dataset query.
 */
public class ExecuteQueryCommand extends AbstractAuthCommand implements Categorized {

  private static final long DEFAULT_TIMEOUT_MIN = Long.MAX_VALUE;
  private final QueryClient queryClient;

  @Inject
  public ExecuteQueryCommand(QueryClient queryClient, CLIConfig cliConfig) {
    super(cliConfig);
    this.queryClient = queryClient;
  }

  @Override
  public void perform(Arguments arguments, PrintStream output) throws Exception {
    String query = arguments.get(ArgumentName.QUERY.toString());
    long timeOutMins = arguments.getLongOptional(ArgumentName.TIMEOUT.toString(), DEFAULT_TIMEOUT_MIN);

    ListenableFuture<ExploreExecutionResult> future = queryClient.execute(cliConfig.getCurrentNamespace().toId(),
                                                                          query);
    try {
      ExploreExecutionResult executionResult = future.get(timeOutMins, TimeUnit.MINUTES);
      if (!executionResult.canContainResults()) {
        output.println("SQL statement does not output any result.");
        executionResult.close();
        return;
      }

      final List<ColumnDesc> schema = executionResult.getResultSchema();
      String[] header = new String[schema.size()];
      for (int i = 0; i < header.length; i++) {
        ColumnDesc column = schema.get(i);
        // Hive columns start at 1
        int index = column.getPosition() - 1;
        header[index] = column.getName() + ": " + column.getType();
      }
      List<QueryResult> rows = Lists.newArrayList(executionResult);
      executionResult.close();

      QueryStatus.OpStatus opStatus = executionResult.getStatus().getStatus();
      if (opStatus != QueryStatus.OpStatus.FINISHED) {
        throw new SQLException(String.format("Query '%s' execution did not finish successfully. " +
                                               "Got final state - %s", query, opStatus));
      }

      Table table = Table.builder()
        .setHeader(header)
        .setRows(rows, new RowMaker<QueryResult>() {
          @Override
          public List<?> makeRow(QueryResult object) {
            return object.getColumns();
          }
        }).build();
      cliConfig.getTableRenderer().render(cliConfig, output, table);

      output.printf("Fetched %d rows", rows.size()).println();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (ExecutionException e) {
      Throwable t = Throwables.getRootCause(e);
      if (t instanceof HandleNotFoundException) {
        throw Throwables.propagate(t);
      }
      throw new SQLException(Throwables.getRootCause(e));
    } catch (CancellationException e) {
      throw new RuntimeException("Query has been cancelled on ListenableFuture object.");
    } catch (TimeoutException e) {
      output.println("Couldn't obtain results after " + timeOutMins + "mins.");
    }

  }

  @Override
  public String getPattern() {
    return String.format("execute <%s> [<%s>]", ArgumentName.QUERY, ArgumentName.TIMEOUT);
  }

  @Override
  public String getDescription() {
    return String.format("Executes %s with optional <%s> in minutes (default is no timeout).",
                         Fragment.of(Article.A, ElementType.QUERY.getName()), ArgumentName.TIMEOUT);
  }

  @Override
  public String getCategory() {
    return CommandCategory.EXPLORE.getName();
  }
}
