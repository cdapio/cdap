# Normalize


Description
-----------
Normalize is a transform plugin that breaks one source row into multiple target rows.
Attributes stored in the columns of a table or a file may need to be broken into multiple
records: for example, one record per column attribute. In general, the plugin allows the
conversion of columns to rows.

Use Case
--------
The normalize transform can be used if you want to reduce the restructuring of a dataset
when a new type of data is introduced into the collection. For example, assume you are
building a master customer table that aggregates data for a user from multiple sources,
and each of the sources has its own type of data to be added to a "customer-id". Instead
of creating wide columns, normalization allows you to transform data into its canonical
form and update the master customer profile simultaneously from the multiple sources.

Properties
----------
**fieldMapping:** A string that is a comma-separated list of field names. Specifies the input schema field
to be mapped to the output schema field. Example: "CustomerID:ID" maps the value of the
CustomerID field to the ID field of the output schema.

**fieldNormalizing:** A string that is a comma-separated list of field names, a common
column for the field types, and a common column for the field values. Specifies the name
of the field to be normalized, to which output field its name should be mapped as a type,
and the output field where the value needs to be saved.

Example: "ItemId:AttributeType:AttributeValue" will save the name "ItemId" to the
"AttributeType" field, and the value of "ItemId" column will be saved in the
"AttributeValue" field of the output schema.

**outputSchema:** The output schema for the data.

Example
-------
This example creates a customer profile table from two sources. Assume we have as sources
a "Customer_Profile" table and a "Customer_Purchase" table which we need to normalize into
a "Customer" table.

Customer_Profile table:

    +==============================================================================================================+
    | CustomerId | First_Name | Last_Name  | Shipping_Address | Credit_Card | Billing_Address | Last_Update_Date   |
    +==============================================================================================================+
    | S23424242  | Joltie     | Root       | 32826 Mars Way,  | 2334-232132 | 32826 Mars Way, | 05/12/2015         |
    |            |            |            | Marsville,  MR,  | -2323       | Marsville,  MR, |                    |
    |            |            |            | 24344            |             | 24344           |                    |
    +--------------------------------------------------------------------------------------------------------------+
    | R45764646  | Iris       | Cask       | 32423, Your Way, | 2343-12312- | 32421 MyVilla,  | 04/03/2012         |
    |            |            |            | YourVille, YR,   | 12313       | YourVille, YR,  |                    |
    |            |            |            | 65765            |             | 23423           |                    |
    +==============================================================================================================+

Map the "CustomerId" column to the "ID" column of the output schema, and the
"Last_Update_Date" to the "Date" column of the output schema. Normalize the "First_Name",
"Last_Name", "Credit_Card", and "Billing_Address" columns by mapping each column name to
the "Attribute_Type" column and their values to the "Attribute_Value" column of the output
schema.

The plugin's JSON Representation will be:

    {
        "name": "Normalize",
        "plugin": {
            "name": "Normalize",
            "type": "transform",
            "label": "Normalize",
            "properties": {
               "fieldMapping": "CustomerId:ID,Last_Update_Date:Date",
               "fieldNormalizing": "First_Name:Attribute_Type:Attribute_Value,
                                    Last_Name:Attribute_Type:Attribute_Value,
                                    Credit_Card:Attribute_Type:Attribute_Value,
                                    Billing_Address:Attribute_Type:Attribute_Value",
               "outputSchema": "{
                             \"type\":\"schema\",
                             \"name\":\"outputSchema\",
                             \"fields\":[
                               {\"name\":\"ID\",\"type\":\"string\"},
                               {\"name\":\"Date\",\"type\":\"string\"},
                               {\"name\":\"Attribute_Type\",\"type\":\"string\"},
                               {\"name\":\"Attribute_Value\",\"type\":\"string\"}
                             ]
               }"
            }
        }
    }


After the transformation, the output records in the Customer table will be:

    +====================================================================================+
    | ID        | Attribute_Type  | Attribute_Value                         | Date       |
    +====================================================================================+
    | S23424242 | First Name      | Joltie                                  | 05/12/2015 |
    | S23424242 | Last Name       | Root                                    | 05/12/2015 |
    | S23424242 | Credit Card     | 2334-232132-2323                        | 05/12/2015 |
    | S23424242 | Billing Address | 32826 Mars Way, Marsville,  MR, 24344   | 05/12/2015 |
    | R45764646 | First Name      | Iris                                    | 04/03/2012 |
    | R45764646 | Last Name       | Cask                                    | 04/03/2012 |
    | R45764646 | Credit Card     | 2343-12312-12313                        | 04/03/2012 |
    | R45764646 | Billing Address | 32421, MyVilla Ct, YourVille, YR, 23423 | 04/03/2012 |
    +====================================================================================+

Next, create a new pipeline to normalize the Customer_Purchase table to the revised Customer table.

Customer_Purchase table:

    +===========================================================+
    | CustomerId | Item_ID          | Item_Cost | Purchase_Date |
    +===========================================================+
    | S23424242  | UR-AR-243123-ST  | 245.67    | 08/09/2015    |
    | S23424242  | SKU-234294242942 | 67.90     | 10/12/2015    |
    | R45764646  | SKU-567757543532 | 14.15     | 06/09/2014    |
    +===========================================================+

Map the "CustomerId" column to the "ID" column of the output schema, and the
"Purchase_Date" to the "Date" column of the output schema. Normalize the "Item_ID" and
"Item_Cost" columns so that each column name will be mapped to the "Attribute_Type" column
and each value will be mapped to the "Attribute_Value" column of the output schema.

The plugin's JSON Representation will be:

    {
        "name": "Normalize",
        "plugin": {
            "name": "Normalize",
            "type": "transform",
            "label": "Normalize",
            "properties": {
               "fieldMapping": "CustomerId:ID,Purchase_Date:Date",
               "fieldNormalizing": "Item_ID:Attribute_Type:Attribute_Value,Item_Cost:Attribute_Type:Attribute_Value",
               "outputSchema": "{
                             \"type\":\"schema\",
                             \"name\":\"outputSchema\",
                             \"fields\":[
                               {\"name\":\"ID\",\"type\":\"string\"},
                               {\"name\":\"Date\",\"type\":\"string\"},
                               {\"name\":\"Attribute_Type\",\"type\":\"string\"},
                               {\"name\":\"Attribute_Value\",\"type\":\"string\"}
                             ]
               }"
            }
        }
    }

After the transformation, the output records in the Customer table will be:

    +=====================================================================================+
    | ID        | Attribute_Type  | Attribute_Value                         | Date        |
    +=====================================================================================+
    | S23424242 | First_Name      | Joltie                                  | 05/12/2015  |
    | S23424242 | Last_Name       | Root                                    | 05/12/2015  |
    | S23424242 | Credit_Card     | 2334-232132-2323                        | 05/12/2015  |
    | S23424242 | Billing_Address | 32826 Mars Way, Marsville,  MR, 24344   | 05/12/2015  |
    | R45764646 | First_Name      | Iris                                    | 04/03/2012  |
    | R45764646 | Last_Name       | Cask                                    | 04/03/2012  |
    | R45764646 | Credit_Card     | 2343-12312-12313                        | 04/03/2012  |
    | R45764646 | Billing_Address | 32421, MyVilla Ct, YourVille, YR, 23423 | 08/09/2015  |
    | S23424242 | Item_ID         | UR-AR-243123-ST                         | 08/09/2015  |
    | S23424242 | Item_Cost       | 245.67                                  | 08/09/2015  |
    | S23424242 | Item_ID         | SKU-234294242942                        | 10/12/2015  |
    | S23424242 | Item_Cost       | 67.90                                   | 10/12/2015  |
    | R45764646 | Item_ID         | SKU-567757543532                        | 06/09/2014  |
    | R45764646 | Item_Cost       | 14.15                                   | 06/09/2014  |
    +=====================================================================================+

---
- CDAP Pipelines Plugin Type: transform
- CDAP Pipelines Version: 1.7.0
