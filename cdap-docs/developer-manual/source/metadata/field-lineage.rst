.. meta::
    :author: Cask Data, Inc.
    :copyright: Copyright © 2018 Cask Data, Inc.

===================
Field Level Lineage
===================


.. _metadata-fieldlevellineage:

Introduction
============
CDAP provides a way to retrieve the lineage for dataset entities. A dataset entity can have associated schema.
The schema defines different fields in the dataset along with their data type information.
**Field Level Lineage** allows a user to get more granular lineage view of a dataset. A field lineage for a
given dataset shows for the specified time range all the fields that were computed for a dataset and the fields
from source datasets that participated in computation of those fields. Field lineage also shows the detail
operations that caused the transformation from fields of a source dataset to the field of a given dataset.

Example Use Case
================
Data analytics group in an Airline company wants to perform analytical queries on the passenger
information stored on SFTP servers. To run the workload, passenger information is imported to Hadoop.
Because the data can come from a variety of sources, the data is normalized into standard format while
importing. PII information is also obfuscated.

By looking at the dataset in Hadoop, a data officer or Business Analyst wants to understand the meaning of the field
"fullName" by reading how it was produced. For example, a data officer might want to know that the "fullName"
field was created by concatenating the "firstName" and "lastName" fields, both of which were extracted as positional
fields from a CSV record in the source named "passengerList". Additionally, typically in a triage or debugging
scenario, an operation team or developer wants to identify how a field was computed when fields show up with wrong
values.

Concepts and Terminology
========================

- **Field** : Field identifies column in a dataset. Field has a name and data type.
- **EndPoint** : EndPoint defines the source or destination of the data along with its namespace from where the fields are read or written to.
- **Field Operation** : Operation defines a single computation on a field. It has a name and description.
- **Read Operation** : Type of operation that reads from the source EndPoint and creates collection of fields.
- **Transform Operation** : Type of operation that transforms collection of input fields to collection of output fields.
- **Write Operation** : Type of operation that writes the collection of fields to the destination EndPoint.
- **Origin** : Origin of the field is the name of the operation that outputted the field. The <origin, fieldName> pair is used to uniquely identify the field because the field can appear in the outputs of multiple operations.

Field Lineage for CDAP Programs
===============================
Field Lineage recording is supported from MapReduce and Spark programs.

**Note:** All the operations that CDAP programs record must have unique names.

Consider a simple MapReduce program that reads the passenger information stored in CSV files and normalizes
the data into standard format before storing it in the CDAP dataset. The program assumes that the fields in the
CSV files are as follows: id, firstName, lastName, address. The program then concatenate firstName and lastName to
create fullName. The address field is normalized into standard format. In this case, the program can record the
following field operations:

The ``ReadOperation`` to represent read from ``passengerList`` file to create fields id, firstName, lastName,
and address::

    Operation read = new ReadOperation("Read", "Read passenger information", EndPoint.of("ns", "passengerList"),
                                       "id", "firstName", "lastName", "address");

The ``TransformOperation`` to represent the concatenation of fields firstName and lastName::

    Operation concat = new TransformOperation("Concat", "Concatenated fields",
                                              Arrays.asList(InputField.of("Read", "firstName"),
                                              InputField.of("Read", "lastName")), "fullName");

Another ``TransformOperation`` to represent the normalization of address::

    Operation normalize = new TransformOperation("Normalize", "Normalized field",
                                                 Collections.singletonList(InputField.of("Read", "address")),
                                                 "address");

Finally ``WriteOperation`` to represent the fields are being written to the example CDAP dataset ``passenger``:::

    Operation write = new WriteOperation("Write", "Wrote to passenger dataset", EndPoint.of("ns", "passenger"),
                                         Arrays.asList(InputField.of("Read", "id"),
                                                       InputField.of("Concat", "fullName"),
                                                       InputField.of("Normalize", "address")));

Note that both ``TransformOperation`` and ``WriteOperation`` take list of ``InputField`` as a parameter representing
input fields to the operation. ``InputField`` is a pair of origin and field name that identifies the input
unambiguously. For example, the ``Write`` operation above uses the address field that ``Normalize`` operation generates,
not the one that ``Read`` operation generates.

The ``initialize()`` method, which is invoked at runtime before a MapReduce or Spark program is executed,
can submit the Field operations created above to the platform::

    @Override
    public void initialize() throws Exception {
      MapReduceContext context = getContext();
      List<Operation> operations = new ArrayList();

      Operation read = new ReadOperation("Read", "Read passenger information", EndPoint.of("ns", "passengerList"),
                                         "id", "firstName", "lastName", "address");
      operations.add(read);

      Operation concat = new TransformOperation("Concat", "Concatenated fields",
                                                Arrays.asList(InputField.of("Read", "firstName"),
                                                InputField.of("Read", "lastName")), "fullName");
      operations.add(concat);

      Operation normalize = new TransformOperation("Normalize", "Normalized field",
                                                   Collections.singletonList(InputField.of("Read", "address")),
                                                   "address");
      operations.add(normalize);

      Operation write = new WriteOperation("Write", "Wrote to passenger dataset", EndPoint.of("ns", "passenger"),
                                           Arrays.asList(InputField.of("Read", "id"),
                                                         InputField.of("Concat", "fullName"),
                                                         InputField.of("Normalize", "address")));
      operations.add(write);

      // Record field operation
      context.record(operations);
    }

Field Lineage for CDAP Data Pipelines
=====================================
Plugins in CDAP data pipelines can also record the field lineage. Currently, plugins of type ``batchsource``,
``transform``, and ``batchsink`` are supported. The capability to record lineage is available in the ``prepareRun()``
method of the plugin by using the context provided to the ``prepareRun()`` method.

**Note:** Individual plugins can record multiple operations, and all operations recorded by single plugin must have unique names.

Consider the use case above of importing the passenger information but with a CDAP data pipeline that consists of
File Source, Concatenate, Address Normalizer, and Table Sink plugins.

These plugins can record the following field lineage operations:

File Source plugin can record the ``FieldReadOperation``, which represents reading the passenger information stored
in CSV file to create the following fields id, firstName, lastName, and address. The fields belong to the output schema::

    @Override
    public void prepareRun(BatchSourceContext context) throws Exception {
      if (config.getSchema() != null && config.getSchema().getFields() != null) {
        List<Schema.Field> fields = config.getSchema().getFields();
        // Make sure the schema and fields are non null
        FieldOperation operation = new FieldReadOperation("Read", "Read from files",
                                                          EndPoint.of(context.getNamespace(), config.referenceName),
                                                          fields.stream().map(Schema.Field::getName)
                                                            .collect(Collectors.toList()));
        context.record(Collections.singletonList(operation));
      }
    }

Concatenate plugin concatenates the fields as represented by the plugin config and record the
``FieldTransformOperation`` in its prepareRun method::

    @Override
    public void prepareRun(StageSubmitterContext context) throws Exception {
      FieldOperation operation = new FieldTransformOperation("Concatenate", "Concatenated fields",
                                                             Arrays.asList(config.fieldToConcatenate1,
                                                                           config.fieldToConcatenate2),
                                                             config.newFieldName);
      context.record(Collections.singletonList(operation));
    }

Similarly Address Normalizer plugin can record the ``FieldTransformOperation`` representing address normalization::

    @Override
    public void prepareRun(StageSubmitterContext context) throws Exception {
      FieldOperation operation = new FieldTransformOperation("Normalize", "Normalized field",
                                                             Collections.singletonList(config.fieldToNormalize),
                                                             config.fieldToConcatenate2);
      context.record(Collections.singletonList(operation));
    }

Finally, Table sink can record ``FieldWriteOperation`` representing writing fields to dataset::

    @Override
    public void prepareRun(BatchSinkContext context) throws Exception {
      if (schema.getFields() != null) {
        FieldOperation operation = new FieldWriteOperation("Write", "Wrote to CDAP Table",
                                                           EndPoint.of(context.getNamespace(), "passenger"),
                                                           schema.getFields().stream().map(Schema.Field::getName)
                                                             .collect(Collectors.toList()));
        context.record(Collections.singletonList(operation));
      }
    }
