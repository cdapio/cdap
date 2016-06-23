.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2015 Cask Data, Inc.

.. _table-exploration:

=================
Table Exploration
=================

A ``Table`` is a core dataset. Unlike relational database tables where every
row has the same schema, every row of a Table can have a different set of columns.
Though Tables do not require a schema, in practice they are often written with an
implicit schema. Column names are often strings, with a single data type used
for all values in the same column. If you are using a Table in this way,
you can set a schema as a Table property to enable exploration. The schema will be
applied at read time, allowing you to run ad-hoc queries against the Table. 

Requirements
------------
In order to explore a Table, your Table must meet a few requirements.

- Columns names must be strings.

- All column values for a specific column must be of the same type. For example, a value cannot be a string
  in one row and an integer in another.

- Column values must be of a primitive type.
  A primitive type is one of boolean, int, long, float, double, bytes, or string. 

- Column names must be valid Hive column names. This means they cannot be reserved keywords such as *drop*.
  Please refer to the `Hive language manual <https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL>`__
  for more information about Hive.

Creating an Explorable Table
----------------------------

When creating a ``Table`` in your application, if you set the table's schema property, your Table
will be enabled for exploration after it is created::

  @Override
  public void configure() {
    Schema profileSchema = Schema.recordOf(
      "profile",
      // id, name, and email are never null and are set when a user profile is created
      Schema.Field.of("id", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
      Schema.Field.of("email", Schema.of(Schema.Type.STRING)),
      // login and active are never set when a profile is created but are set later, so they are nullable.
      Schema.Field.of("login", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
      Schema.Field.of("active", Schema.nullableOf(Schema.of(Schema.Type.LONG)))
    );
    createDataset("profiles", Table.class.getName(), DatasetProperties.builder()
      // set the schema property so that it can be explored via Hive
      .add(Table.PROPERTY_SCHEMA, profileSchema.toString())
      // to indicate that the id field should come from the row key and not a row column
      .add(Table.PROPERTY_SCHEMA_ROW_FIELD, "id")
      .build());
  } 

Note that the schema row field property is set along with the schema property. The schema row field property
must be set if you want to explore your Table row along with Table columns. In the example above, this property
will let CDAP know to read the ``id`` field from the Table row instead of from the Table columns. 

Setting a Schema on an Existing Table
-------------------------------------

.. highlight:: console

Since schema is applied at read time, it is possible to set a schema on a Table after it has been created.
It is also possible to change the schema of a Table. Dataset properties can be set using the RESTful API.
For example, the same schema set through the example code above can also be set through the RESTful API
(reformatted to fit):

.. tabbed-parsed-literal::

  $ curl -w"\n" -X PUT "http://example.com:10000/v3/namespaces/<namespace-id>/data/datasets/profiles/properties" \
    -d '{ "typeName": "table", \
          "properties": { \
            "schema": "{ \
              \"type\":\"record\", \
              \"name\":\"purchase\", \
              \"fields\":[ \
                {\"name\":\"id\",\"type\":\"string\"}, \
                {\"name\":\"name\",\"type\":\"string\"}, \
                {\"name\":\"email\",\"type\":\"string\"}, \
                {\"name\":\"login\",\"type\":[\"long\", \"null\"]}, \
                {\"name\":\"active\",\"type\":[\"long\", \"null\"]} \
              ] \
            }", \
            "schema.row.field": "id" \
          } \
        }'
  
CDAP schemas are adopted from the `Avro Schema Declaration <http://avro.apache.org/docs/1.7.3/spec.html#schemas>`__.
Note that since dataset properties must be strings, the schema JSON has to be escaped properly.

Formulating Queries
-------------------
When creating your queries, keep these limitations in mind:

- The query syntax of CDAP is a subset of the variant of SQL that was first defined by Apache Hive.
- The SQL commands ``UPDATE`` and ``DELETE`` are not allowed on CDAP datasets.
- When addressing your datasets in queries, you need to prefix the Table name with ``dataset_``.
  For example, if your Table is named ``Purchases``, then the corresponding Hive table
  name is ``dataset_purchases``. Note that the table name is lower-case.
- If your Table name contains a '.' or a '-', those characters will be converted to '_' for the Hive
  table name. For example, if your Table is named ``my-table.name``, the corresponding Hive table
  name will be ``dataset_my_table_name``. Beware of name collisions. For example, 
  ``my.table`` will use the same Hive table name as ``my_table``. Beware of name collisions.
  For example, ``my.table`` will use the same Hive table name as ``my_table``.

For more examples of queries, please refer to the `Hive language manual
<https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DML>`__.
