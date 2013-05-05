package com.continuuity.passport;

/**
 *  Constants used in the code internally.
 */
public class Constants {

  public static final String CFG_JDBC_TYPE = "passport.jdbc.type";
  public static final String DEFAULT_JDBC_TYPE = "mysql";
  public static final String CFG_JDBC_CONNECTION_STRING = "passport.jdbc.connection.string";
  public static final String DEFAULT_JDBC_CONNECTION_STRING = "jdbc:mysql://localhost:3306/passport?user=root" +
                                                              "&zeroDateTimeBehavior=convertToNull";

  public static final String CFG_SERVER_PORT = "passport.http.server.port";
  public static final int DEFAULT_SERVER_PORT = 7777;
  public static final String CFG_GRACEFUL_SHUTDOWN_TIME = "passport.http.graceful.shutdown.time";
  public static final int DEFAULT_GRACEFUL_SHUTDOWN_TIME =  10000;
  public static final String CFG_HTTP_MAX_THREADS = "passport.http.max.threads";
  public static final int DEFAULT_HTTP_MAX_THREADS = 100;
  public static final String CFG_PROFANE_WORDS_FILE_PATH = "passport.profane.words.path";
  public static final String DEFAULT_PROFANE_WORDS_FILE_PATH = "";
  public static final int CONNECTION_POOL_SIZE = 100;
  public static final String NAMED_DB_CONNECTION_POOL_BINDING = "Passport connection pool binding";
}
