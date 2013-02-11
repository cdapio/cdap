package com.continuuity.app.program;

import java.net.URI;

/**
 *
 */
public class Program<S> {

  /**
   * Defines types of programs supported by the system.
   */
  public enum Type {
    FLOW(1),
    PROCEDURE(2),
    BATCH(3);

    private final int programType;

    private Type(int type) {
      this.programType = type;
    }
  }

  /**
   * Defines the type of the {@link Program}
   */
  private final Type type;

  /**
   * Defines version of a program.
   */
  private final Version version;

  /**
   * Specification for this program.
   */
  private final S specification;

  /**
   * URI of the program.
   */
  private final URI uri;

  /**
   * Constructs an instance of {@link Program}
   * @param type
   */

  /**
   * Constructs an instance of {@link Program}
   *
   * @param specification of the this Program
   * @param type of this Program
   * @param version of this Program
   * @param uri to the Program
   */
  public Program(S specification, Type type, Version version, URI uri) {
    this.specification = specification;
    this.type = type;
    this.version = version;
    this.uri = uri;
  }

  /**
   * @return Type of this Program
   */
  public Type getType() {
    return type;
  }

  /**
   * @return Version of this Program
   */
  public Version getVersion() {
    return version;
  }

  /**
   * @return Specification of this Program
   */
  public S getSpecification() {
    return specification;
  }

  /**
   * @return Path to this Program
   */
  public URI getUri() {
    return uri;
  }
}
