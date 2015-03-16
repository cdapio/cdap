.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _object-mapped-table-exploration:

============================================
ObjectMappedTable Exploration
============================================

An ``ObjectMappedTable`` is a system Dataset that can write Java objects to a Table
by mapping object fields to Table columns. It can also be explored in an ad-hoc manner.

Creating an ObjectMappedTable
-----------------------------

When creating an ``ObjectMappedTable`` in your application, you must specify the Java type
that will be stored in your table::

  @Override
  public void configure() {
    try {
      createDataset("purchases", ObjectMappedTable.class,
                    ObjectMappedTableProperties.builder()
                      .setType(Purchase.class)
                      .build()
                   );
    } catch (UnsupportedTypeException e) {
      // This exception is thrown by ObjectMappedTable if its parameter type cannot be
      // (de)serialized (for example, if it is an interface and not a class, then there is
      // no auto-magic way deserialize an object.) In this case that will not happen
      // because Purchase is an actual class.
      throw new RuntimeException(e);
    }
  } 

CDAP will derive the record schema from the specified type. For example, if the ``Purchase`` class is defined as::

  public class Purchase {
    private final String customer, product;
    private final int quantity, price;
    private final long purchaseTime;
    private String catalogId;

    public Purchase(String customer, String product, int quantity, int price, long purchaseTime) {
      this.customer = customer;
      this.product = product;
      this.quantity = quantity;
      this.price = price;
      this.purchaseTime = purchaseTime;
      this.catalogId = "";
    }
  }

CDAP will map each object field to a Table column and the schema will be::

  (key BINARY, catalogid STRING, customer STRING, price INT, product STRING, purchasetime BIGINT, quantity INT)

Note that all column names have been changed to lowercase letters. This is because Hive column names are case-insensitive.
In addition to the object fields, the object key has been inserted into the schema as a binary column ``key``.

If you wish to name the key column differently, perhaps because your object already contains a field named "key", you 
can do so by providing an additional property when creating your Dataset. You can also set the key type to ``STRING``
instead of ``BINARY`` if desired::
  
  @Override
  public void configure() {
    try {
      createDataset("purchases", ObjectMappedTable.class,
                    ObjectMappedTableProperties.builder()
                      .setType(Purchase.class)
                      .setRowKeyExploreName("rowkey")
                      // only STRING and BINARY are supported.
                      .setRowKeyExploreType(Schema.Type.STRING)
                      .build()
                   );
    } catch (UnsupportedTypeException e) {
      // This exception is thrown by ObjectMappedTable if its parameter type cannot be
      // (de)serialized (for example, if it is an interface and not a class, then there is
      // no auto-magic way deserialize an object.) In this case that will not happen
      // because Purchase is an actual class.
      throw new RuntimeException(e);
    }
  } 

Creating the Dataset in this manner would result in a different column name and type for the object key:: 

  (rowkey STRING, catalogid STRING, customer STRING, price INT, product STRING, purchasetime BIGINT, quantity INT)

.. _object-mapped-table-exploration-sql-limitations:

Limitations
-----------
* The record type must be a structured type, that is, a Java class with fields. This is because SQL tables require
  a structure type at the top level. The fields must be primitives. That is, they must be an int, Integer,
  float, Float, double, Double, bool, Boolean, String, byte[], or ByteBuffer. UUID is supported and
  will translate into a binary field.

* The record type must be that of an actual Java class, not an interface. The reason is that interfaces only define
  methods but not fields; hence, reflection would not be able to derive any fields or types from the interface.

* Fields of a class that are declared static or transient are ignored during schema generation. This means that the
  record type must have at least one non-transient and non-static field. For example,
  the ``java.util.Date`` class has only static and transient fields. Therefore a record type of ``Date`` is not
  supported and will result in an exception when the Dataset is created.

* You cannot insert data into an ``ObjectMappedTable`` using SQL.

Formulating Queries
-------------------
When creating your queries, keep these limitations in mind:

.. TODO(CDAP-1671): update with namespaces

- The query syntax of CDAP is a subset of the variant of SQL that was first defined by Apache Hive.
- The SQL commands ``UPDATE`` and ``DELETE`` are not allowed on CDAP Datasets.
- When addressing your datasets in queries, you need to prefix the data set name with the CDAP
  namespace ``cdap_user_``. For example, if your Dataset is named ``Purchases``, then the corresponding table
  name is ``cdap_user_purchases``. Note that the table name is lower-case.

For more examples of queries, please refer to the `Hive language manual
<https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DML>`__.
