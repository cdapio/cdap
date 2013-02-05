package com.continuuity.api.procedure;

/**
 *
 */
public class ProcedureSpecification {

  private final String name;
  private final String description;

  private ProcedureSpecification(String name, String description) {
    this.name = name;
    this.description = description;
  }

  public String getName() {
    return name;
  }

  public String getDescription() {
    return description;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static final class Builder {

    private String name;
    private String description;

    public DescriptionSetter setName(String name) {
      this.name = name;
      return new DescriptionSetter();
    }

    public final class DescriptionSetter {
      public AfterDescription setDescription(String description) {
        Builder.this.description = description;
        return new AfterDescription();
      }
    }

    public final class AfterDescription {

      @Deprecated
      public ProcedureSpecification build() {
        return new ProcedureSpecification(name, description);
      }
    }

    private Builder() {}
  }
}
