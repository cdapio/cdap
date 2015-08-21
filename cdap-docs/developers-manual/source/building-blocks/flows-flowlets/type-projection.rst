.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2014 Cask Data, Inc.

===============
Type Projection
===============

flowlets perform an implicit projection on the input objects if they do
not match exactly what the process method accepts as arguments. This
allows you to write a single process method that can accept multiple
**compatible** types. For example, if you have a process method::

  @ProcessInput
  count(String word) {
    ...
  }

and you send data of type ``Long`` to this flowlet, then that type does
not exactly match what the process method expects. You could now write
another process method for ``Long`` numbers::

  @ProcessInput count(Long number) {
    count(number.toString());
  }

and you could do that for every type that you might possibly want to
count, but that would be rather tedious. Type projection does this for
you automatically. If no process method is found that matches the type
of an object exactly, it picks a method that is compatible with the
object.

In this case, because Long can be converted into a String, it is
compatible with the original process method. Other compatible
conversions are:

- Every primitive type that can be converted to a ``String`` is compatible with
  ``String``.
- Any numeric type is compatible with numeric types that can represent it.
  For example, ``int`` is compatible with ``long``, ``float`` and ``double``,
  and ``long`` is compatible with ``float`` and ``double``, but ``long`` is not
  compatible with ``int`` because ``int`` cannot represent every ``long`` value.
- A byte array is compatible with a ``ByteBuffer`` and vice versa.
- A collection of type A is compatible with a collection of type B,
  if type A is compatible with type B.
  Here, a collection can be an array or any Java ``Collection``.
  Hence, a ``List<Integer>`` is compatible with a ``String[]`` array.
- Two maps are compatible if their underlying types are compatible.
  For example, a ``TreeMap<Integer, Boolean>`` is compatible with a
  ``HashMap<String, String>``.
- Other Java objects can be compatible if their fields are compatible.
  For example, in the following class ``Point`` is compatible with ``Coordinate``,
  because all common fields between the two classes are compatible.
  When projecting from ``Point`` to ``Coordinate``, the color field is dropped,
  whereas the projection from ``Coordinate`` to ``Point`` will leave the ``color`` field
  as ``null``::

    class Point {
      private int x;
      private int y;
      private String color;
    }

    class Coordinates {
      int x;
      int y;
    }

Type projections help you keep your code generic and reusable. They also
interact well with inheritance. If a flowlet can process a specific
object class, then it can also process any subclass of that class.

