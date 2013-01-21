package com.continuuity.api.data;

/**
 * This is the abstract base class for all data sets. A data set is an
 * implementation of a data pattern that can be reused across programs and
 * applications. The life cycle of a data set is as follows:
 * <li>An application declares the data sets that its programs use.</li>
 * <li>When the application is deployed, the DataSet object is created and
 *   its configure() method is called. This method returns a specification
 *   that contains all information needed to instantiate the data set at
 *   runtime.</li>
 * <li>At runtime (in a flow or procedure) the data set is instantiated
 *   by calling a factory method of the execution context. This uses the
 *   constructor of the the data set that takes the above specification
 *   as argument. It is important that the data set is instantiated through
 *   the context: This also makes sure that the data fabric runtime is
 *   properly injected into the data set.
 *   </li>
 * <li>Hence every DataSet must implement a configure() method and a
 *   constructor from DataSetSpecification.</li>
 */
public abstract class DataSet {

  private final String name;

  /** get the name of this data set */
  public final String getName() {
    return this.name;
  }

  /** base constructor that only sets the name of the data set */
  public DataSet(String name) {
    this.name = name;
  }

  /** constructor to instantiate the data set at runtime */
  public DataSet(DataSetSpecification spec) {
    this(spec.getName());
  }

  /**
   * This method is called at deployment time and must return the complete
   * specification that is needed to instantiate the data set at runtime
   * (@see DataSet(DataSetSpecification)).
   * @return a builder that has all meta data needed for runtime instantiation
   */
  public abstract DataSetSpecification.Builder configure();
}
