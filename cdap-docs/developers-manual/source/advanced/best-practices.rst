.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014 Cask Data, Inc.

==========================================
Best Practices for Developing Applications
==========================================

Initializing Instance Fields
============================
There are three ways to initialize instance fields used in flowlets:

#. Using the default constructor;
#. Using the ``initialize()`` method of the flowlets; and
#. Using ``@Property`` annotations.

To initialize using a property annotation, simply annotate the field definition with
``@Property``. 

The following example demonstrates the convenience of using ``@Property`` in a
``WordFilter`` flowlet
that filters out specific words::

  public static class WordFilter extends AbstractFlowlet {

    private OutputEmitter<String> out;

    @Property
    private final String toFilterOut;

    public WordFilter(String toFilterOut) {
      this.toFilterOut = toFilterOut;
    }

    @ProcessInput()
    public void process(String word) {
      if (!toFilterOut.equals(word)) {
        out.emit(word);
      }
    }
  }


The flowlet constructor is called with the parameter when the flow is configured::

  public static class WordCountFlow extends AbstractFlow {

    @Override
    public void configureFlow() {
      setName("WordCountFlow");
      setDescription("Flow for counting words");
      addFlowlet("Tokenizer", new Tokenizer());
      addFlowlet("WordsFilter", new WordsFilter("the"));
      addFlowlet("WordsCounter", new WordsCounter());
      connectStream("text", "Tokenizer");
      connect("Tokenizer", "WordsFilter");
      connect("WordsFilter", "WordsCounter");
    }
  }


At run-time, when the flowlet is started, a value is injected into the ``toFilterOut``
field.

Field types that are supported using the ``@Property`` annotation are primitives,
boxed types (e.g. ``Integer``), ``String`` and ``enum``.


Scanning Over Tables with ``byte[]`` Keys
=========================================
In the CDAP Java API, 
`Bytes.stopKeyForPrefix() <../../reference-manual/javadocs/co/cask/cdap/api/common/Bytes.html#stopKeyForPrefix(byte[])>`__
is a very handy tool when performing scans over tables with ``byte[]`` keys.

The method returns a given prefix, incremented by one, in a form that is suitable for
prefix matching. Here is an example, showing ``stopKeyForPrefix`` being used to generate,
from a provided index key, an incremented stop key::

  // Returns first that matches
  @Nullable
  public <T> T get(Key id, Class<T> classOfT) {
    try {
      Scanner scan = table.scan(id.getKey(), Bytes.stopKeyForPrefix(id.getKey()));
      Row row = scan.next();
      if (row == null || row.isEmpty()) {
        return null;
      }

      byte[] value = row.get(COLUMN);
      if (value == null) {
        return null;
      }

      return deserialize(value, classOfT);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

