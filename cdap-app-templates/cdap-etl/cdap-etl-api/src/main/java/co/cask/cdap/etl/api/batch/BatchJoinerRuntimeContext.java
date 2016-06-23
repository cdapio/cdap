package co.cask.cdap.etl.api.batch;

import co.cask.cdap.api.data.schema.Schema;

import java.util.Map;

/**
 * Runtime context for batch joiner
 */
public interface BatchJoinerRuntimeContext extends BatchRuntimeContext {

  /**
   * Retruns list of schemas for batch joiner
   * @return
   */
  Map<String, Schema> getInputSchemas();
}
