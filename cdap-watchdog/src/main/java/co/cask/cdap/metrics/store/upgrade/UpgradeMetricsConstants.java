package co.cask.cdap.metrics.store.upgrade;

/**
 *
 */
public final class UpgradeMetricsConstants {
  public static final String DEFAULT_ENTITY_TABLE_NAME_V1 = "metrics.entity";

  // for migration purpose
  public static final String EMPTY_TAG = "-";
  public static final int DEFAULT_CONTEXT_DEPTH = 6;
  public static final int DEFAULT_METRIC_DEPTH = 4;
  public static final int DEFAULT_TAG_DEPTH = 3;
}
