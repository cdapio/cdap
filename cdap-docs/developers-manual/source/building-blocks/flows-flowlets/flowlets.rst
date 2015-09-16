.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014 Cask Data, Inc.

.. _flowlets:

========
Flowlets
========

*Flowlets*, the basic building blocks of a flow, represent each
individual processing node within a flow. Flowlets consume data objects
from their inputs and execute custom logic on each data object, allowing
you to perform data operations as well as emit data objects to the
flowlet’s outputs. Flowlets specify an ``initialize()`` method, which is
executed at the startup of each instance of a flowlet before it receives
any data.

.. _flowlets_RoundingFlowlet:

The example below shows a flowlet that reads *Double* values, rounds
them, and emits the results. It has a simple configuration method and
doesn't do anything for initialization or destruction::

  public class RoundingFlowlet extends AbstractFlowlet {

    @Override
    public void configure(FlowletConfigurer configurer) {
      super.configure(configurer);
      setName("round");
      setDescription("A rounding flowlet");
    }

    @Override
    public void initialize(FlowletContext context) throws Exception {
    }

    @Override
    public void destroy() {
    }

    OutputEmitter<Long> output;
    @ProcessInput
    public void round(Double number) {
      output.emit(Math.round(number));
    }


The most interesting method of this flowlet is ``round()``, the method
that does the actual processing. It uses an output emitter to send data
to its output. This is the only way that a flowlet can emit output to
another connected flowlet::

  OutputEmitter<Long> output;
  @ProcessInput
  public void round(Double number) {
    output.emit(Math.round(number));
  }

Note that the flowlet declares the output emitter but does not
initialize it. The flow system initializes and injects its
implementation at runtime.

The method is annotated with ``@ProcessInput`` |---| this tells the flow
system that this method can process input data.

You can overload the process method of a flowlet by adding multiple
methods with different input types. When an input object comes in, the
flowlet will call the method that matches the object’s type::

  OutputEmitter<Long> output;

  @ProcessInput
  public void round(Double number) {
    output.emit(Math.round(number));
  }
  @ProcessInput
  public void round(Float number) {
    output.emit((long)Math.round(number));
  }

If you define multiple process methods, a method will be selected based
on the input object’s origin; that is, the name of a stream or the name
of an output of a flowlet.

A flowlet that emits data can specify this name using an annotation on
the output emitter. In the absence of this annotation, the name of the
output defaults to “out”::

  @Output("code")
  OutputEmitter<String> out;

Data objects emitted through this output can then be directed to a
process method of a receiving flowlet by annotating the method with the
origin name::

  @ProcessInput("code")
  public void tokenizeCode(String text) {
    ... // perform fancy code tokenization
  }
