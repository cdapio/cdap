.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014 Cask Data, Inc.

============================================
Runtime Arguments
============================================

Flows, Procedures, MapReduce Jobs, and Workflows can receive runtime arguments:

- For Flows and Procedures, runtime arguments are available to the ``initialize`` method in the context.

- For MapReduce, runtime arguments are available to the ``beforeSubmit`` and ``onFinish`` methods in the context.
  The ``beforeSubmit`` method can pass them to the Mappers and Reducers through the job configuration.

- When a Workflow receives runtime arguments, it passes them to each MapReduce in the Workflow.

The ``initialize()`` method in this example accepts a runtime argument for the
``HelloWorld`` Procedure. For example, we can change the greeting from
the default “Hello” to a customized “Good Morning” by passing a runtime argument::

  public static class Greeting extends AbstractProcedure {

    @UseDataSet("whom")
    KeyValueTable whom;
    private String greeting;

    public void initialize(ProcedureContext context) {
      Map<String, String> args = context.getRuntimeArguments();
      greeting = args.get("greeting");
      if (greeting == null) {
        greeting = "Hello";
      }
    }

    @Handle("greet")
    public void greet(ProcedureRequest request,
                      ProcedureResponder responder) throws Exception {
      byte[] name = whom.read(NameSaver.NAME);
      String toGreet = name != null ? new String(name) : "World";
      responder.sendJson(greeting + " " + toGreet + "!");
    }
  }
