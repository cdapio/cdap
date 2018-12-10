package co.cask.cdap.data.spi.pgsql;

import com.opentable.db.postgres.junit.EmbeddedPostgresRules;
import com.opentable.db.postgres.junit.SingleInstancePostgresRule;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 *
 */
public class PostgreSQLTest {
  @Rule
  public SingleInstancePostgresRule pg = EmbeddedPostgresRules.singleInstance();

  @Test
  public void testPostgreSQL() throws Exception {
    try (Connection c = pg.getEmbeddedPostgres().getPostgresDatabase().getConnection()) {

      String createStatement = "CREATE TABLE RUN_RECORDS " +
        "( " +
        "RUN_ID INT, " +
        "START_TIME BIGINT, " +
        "END_TIME BIGINT, " +
        "PRIMARY KEY (RUN_ID) " +
        ");";

      final int numRecords = 10;
      List<Integer> runIds = new ArrayList<>();
      List<Integer> startTimes = new ArrayList<>();
      List<Integer> endTimes = new ArrayList<>();
      try (Statement s = c.createStatement()) {
        s.executeUpdate(createStatement);

        for (int i = 1; i <= numRecords; i++) {
          runIds.add(i * 10);
          startTimes.add(i * 100);
          endTimes.add(i * 1000);
          s.executeUpdate(String.format("INSERT INTO RUN_RECORDS values(%d, %d, %d)", i * 10, i * 100, i * 1000));
        }

        try (ResultSet rs = s.executeQuery("SELECT * from RUN_RECORDS")) {
          List<Integer> actualRunIds = new ArrayList<>();
          List<Integer> actualStartTimes = new ArrayList<>();
          List<Integer> actualEndTimes = new ArrayList<>();
          while (rs.next()) {
            actualRunIds.add(rs.getInt(1));
            actualStartTimes.add(rs.getInt(2));
            actualEndTimes.add(rs.getInt(3));
          }
          Assert.assertEquals(runIds, actualRunIds);
          Assert.assertEquals(startTimes, actualStartTimes);
          Assert.assertEquals(endTimes, actualEndTimes);
        }

        try (ResultSet rs = s.executeQuery("SELECT RUN_ID from RUN_RECORDS where RUN_ID % 20 = 0")) {
          List<Integer> actualRunIds = new ArrayList<>();
          while (rs.next()) {
            actualRunIds.add(rs.getInt(1));
          }
          Assert.assertEquals(runIds.stream().filter(x -> x % 20 == 0).collect(Collectors.toList()), actualRunIds);
        }
      }
    }
  }
}
