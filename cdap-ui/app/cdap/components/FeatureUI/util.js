import find from 'lodash/find';
import remove from 'lodash/remove';
import isEmpty from 'lodash/isEmpty';

export function toCamelCase(value) {
  return  value.replace(/(\w)(.*?)\b/g,function(result, group1, group2) {
      return result ? (group1.toUpperCase() + (group2 ? group2.toLowerCase():'')): result;
  });
}

export function getPropertyUpdateObj(property, subPropertyName, schemaName, schemaColumns) {
  let updateObj = {
    property: property.paramName,
    schemaName: schemaName,
    schemaColumns: schemaColumns
  };
  if (isEmpty(property.subParams)) {
    updateObj.subProperty = "none";
    updateObj.isSingleSelect = !property.isCollection;
  } else {
    updateObj.subProperty = subPropertyName;
    let subProperty = find(property.subParams, { paramName: subPropertyName });
    updateObj.isSingleSelect = subProperty && !subProperty.isCollection;
  }
  return updateObj;
}

export function updatePropertyMapWithObj(propertyMap, updateObj) {
  let mappedProperty = propertyMap.get(updateObj.property);
  if (mappedProperty) {
    let mappedPropertyValue = find(mappedProperty, { header: updateObj.subProperty });
    if (mappedPropertyValue) {
      if (!updateObj.isSingleSelect) {
        let schemaValueMap = mappedPropertyValue.value;
        if (isEmpty(updateObj.schemaColumns)) {
          schemaValueMap.delete(updateObj.schemaName);
        } else {
          schemaValueMap.set(updateObj.schemaName, updateObj.schemaColumns);
        }
      } else {
        if (isEmpty(updateObj.schemaColumns)) {
          remove(mappedProperty, { header: updateObj.subProperty });
        } else {
          mappedPropertyValue.value = new Map([[updateObj.schemaName, updateObj.schemaColumns]]);
        }
      }
    } else if (!isEmpty(updateObj.schemaColumns)) {
      mappedProperty.push({
        header: updateObj.subProperty,
        isCollection: !updateObj.isSingleSelect,
        value: new Map([[updateObj.schemaName, updateObj.schemaColumns]])
      });
    }
  } else if (!isEmpty(updateObj.schemaColumns)) {
    propertyMap.set(updateObj.property, [{
      header: updateObj.subProperty,
      isCollection: !updateObj.isSingleSelect,
      value: new Map([[updateObj.schemaName, updateObj.schemaColumns]])
    }]);
  }
}

export function getFeatureObject(props) {
  let featureObject = {
    pipelineRunName: props.featureName
  };
  if (!isEmpty(props.selectedSchemas)) {
    featureObject["dataSchemaNames"] = props.selectedSchemas.map(schema => schema.schemaName);
  }
  if (!isEmpty(props.propertyMap)) {
    props.propertyMap.forEach((value, property) => {
      if (value) {
        featureObject[property] = [];
        let subPropObj = {};
        value.forEach(subParam => {
          if (subParam.header == "none") {
            subParam.value.forEach((columns, schema) => {
              if (!isEmpty(columns)) {
                columns.forEach((column) => {
                  if (subParam.isCollection) {
                    featureObject[property].push({
                      table: schema,
                      column: column.columnName
                    });
                  } else {
                    featureObject[property] = {
                      table: schema,
                      column: column.columnName
                    };
                  }
                });
              }
            });
          } else {
            subParam.value.forEach((columns, schema) => {
              if (!isEmpty(columns)) {
                let subPropValue = subParam.isCollection ? [] : {};
                columns.forEach((column) => {
                  if (subParam.isCollection) {
                    subPropValue.push({
                      table: schema,
                      column: column.columnName
                    });
                  } else {
                    subPropValue = {
                      table: schema,
                      column: column.columnName
                    };
                  }
                });
                subPropObj[subParam.header] = subPropValue;
              }
            });
          }
        });
        if (!isEmpty(subPropObj)) {
          featureObject[property].push(subPropObj);
        }
      }
    });
  }
  if (!isEmpty(props.configurationList)) {
    props.configurationList.forEach((configuration) => {
      if (!isEmpty(configuration.value)) {
        switch (configuration.dataType) {
          case 'int':
            if (configuration.isCollection) {
              let values = configuration.value.split(",");
              featureObject[configuration.name] = values.map(value => parseInt(value));
            } else {
              featureObject[configuration.name] = parseInt(configuration.value);
            }
            break;
          default:
            if (configuration.isCollection) {
              featureObject[configuration.name] = configuration.value.split(",");
            } else {
              featureObject[configuration.name] = configuration.value;
            }
        }
      }
    });
  }
  return featureObject;
}

