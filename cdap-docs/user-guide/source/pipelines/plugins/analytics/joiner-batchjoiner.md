# Joiner


Description
-----------
Joins records from one or more input based on join keys. Supports `inner` and `outer` joins, selection and renaming of output fields.  

Use Case
--------
The transform is used when you want to combine fields from one or more input, similar to the joins in SQL.

Properties
----------
**joinKeys:** List of keys to perform the join operation. The list is separated by `&`. 
Join key from each input stage will be prefixed with `<stageName>.` and the relation among join keys from different inputs is represented by `=`. 
For example: customers.customer_id=items.c_id&customers.customer_name=items.c_name means the join key is a composite key
of customer id and customer name from customers and items input stages and join will be performed on equality 
of the join keys. This transform only supports equality for joins.

**selectedFields:** Comma-separated list of fields to be selected and renamed in join output from each input stage. 
Each selected field that should be present in the output must be prefixed with '<input_stage_name>'. 
The syntax for specifying alias for each selected field is similar to sql. 
For example: customers.id as customer_id, customer.name as customer_name, item.id as item_id, <stageName>.inputFieldName as alias. 
The output will have same order of fields as selected in selectedFields. There must not be any duplicate fields in output.

**requiredInputs:** Comma-separated list of stages. Required input stages decide the type of the join. 
If all the input stages are present in required inputs, inner join will be performed. 
Otherwise, outer join will be performed considering non-required inputs as optional.

**numPartitions:** Number of partitions to use when grouping fields. If not specified, the execution
framework will decide on the number to use.

Example
-------
This example inner joins records from ``customers`` and ``purchases`` inputs on customer id and selects customer_id, name, item and price fields.

    {
        "name": "Joiner",
        "type": "batchjoiner",
        "properties": {
            "selectedFields": "customers.id as customer_id,customers.first_name as name,purchases.item,purchases.price",
            "requiredInputs": "customers, purchases",
            "joinKeys": "customers.id = purchases.customer_id"
        }
    }


For example, suppose the joiner receives input records from customers and purchases as below:


    +=================================================================================================+
    | id | first_name | last_name |  street_address      |   city    | state | zipcode | phone number |  
    +=================================================================================================+
    | 1  | Douglas    | Williams  | 1, Vista Montana     | San Jose  | CA    | 95134   | 408-777-3214 |
    | 2  | David      | Johnson   | 3, Baypointe Parkway | Houston   | TX    | 78970   | 804-777-2341 |
    | 3  | Hugh       | Jackman   | 5, Cool Way          | Manhattan | NY    | 67263   | 708-234-2168 |
    | 4  | Walter     | White     | 3828, Piermont Dr    | Orlando   | FL    | 73498   | 201-734-7315 |
    | 5  | Frank      | Underwood | 1609 Far St.         | San Diego | CA    | 29770   | 201-506-8756 |
    | 6  | Serena     | Woods     | 123 Far St.          | Las Vegas | Nv    | 45334   | 888-605-3479 |
    +=================================================================================================+

    +==============================+
    | customer_id | item   | price |      
    +==============================+
    | 1           | donut  | 0.80  |
    | 1           | coffee | 2.05  |
    | 2           | donut  | 1.50  |
    | 2           | plate  | 0.50  |
    | 3           | tea    | 1.99  |
    | 5           | cookie | 0.50  |
    +==============================+

Output records will contain inner join on customer id:

    +========================================+
    | customer_id | name    | item   | price |
    +========================================+
    | 1           | Douglas | donut  | 0.80  |
    | 1           | Douglas | coffee | 2.05  |
    | 2           | David   | donut  | 1.50  |
    | 2           | David   | plate  | 0.50  |
    | 3           | Hugh    | tea    | 1.99  |
    | 5           | Frank   | cookie | 0.50  |
    +========================================+

---
- CDAP Pipelines Plugin Type: batchjoiner
- CDAP Pipelines Version: 1.7.0
