/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

import React from 'react';
import AddSchema from '../AddSchema';
import SchemaSelectorModal from '../SchemaSelectorModal';
import AlertModal from '../AlertModal';
import isEmpty from 'lodash/isEmpty';
import findIndex from 'lodash/findIndex';
import cloneDeep from 'lodash/cloneDeep';
import find from 'lodash/find';
import remove from 'lodash/remove';
import { removeSchemaFromPropertyMap } from '../util';
import PropTypes from 'prop-types';


require('./SchemaSelector.scss');

class SchemaSelector extends React.Component {
  schemas = [];
  constructor(props) {
    super(props);
    this.onAlertClose = this.onAlertClose.bind(this);
    this.onAddSchemaClose = this.onAddSchemaClose.bind(this);
    this.state = {
      openSchemaModal: false,
      openAlertModal: false,
      schemaSelected: null,
      alertMessage: '',
      showSchemaSelector: true,
      operationType: 'ADD',
      schemaDP: []
    };
  }

  openSchemaSelectorModal() {
    this.setState({
      schemaDP: isEmpty(this.props.availableSchemas) ? [] :
        this.props.availableSchemas.map((item) => {
          return {
            schemaName: item.schemaName,
            schemaColumns: item.schemaColumns,
            selected: false
          };
        }),
      openSchemaModal: true,
      showSchemaSelector: true,
      schemaSelected: null,
      operationType: 'ADD'
    });
  }

  performAction(action, data) {
    if (action == 'REMOVE') {
      this.setState({
        schemaSelected: data,
        alertMessage: 'Are you sure you want to delete: ' + data.schemaName,
        openAlertModal: true
      });
    } else if (action == 'EDIT') {
      let selectedSchema = find(this.props.availableSchemas, { schemaName: data.schemaName });
      selectedSchema.schemaColumns = selectedSchema.schemaColumns.map(column => {
        if (find(data.schemaColumns, { columnName: column.columnName })) {
          column.checked = true;
        } else {
          column.checked = false;
        }
        return column;
      });
      this.setState({
        schemaSelected: selectedSchema,
        openSchemaModal: true,
        showSchemaSelector: false,
        operationType: 'EDIT'
      });
    }
  }

  onAddSchemaClose(action, data, type) {
    if (action == 'OK') {
      switch (type) {
        case 'ADD':
          {
            let selectedSchemas = [];
            data.forEach((schema) => {
              let schemaData = find(this.props.availableSchemas, { schemaName: schema });
              if (!isEmpty(schemaData)) {
                selectedSchemas.push({
                  schemaName: schema,
                  selected: true,
                  schemaColumns: schemaData.schemaColumns
                });
              }
            });
            this.props.setSelectedSchemas(selectedSchemas);
          }
          break;
        case 'EDIT': {
          this.props.updateSelectedSchema(data);
        }
          break;
      }

    }
    this.setState({
      openSchemaModal: false,
    });
  }

  isSchemaAlreadyAdded(schemaName) {
    return findIndex(this.props.selectedSchemas, { schemaName: schemaName }) >= 0;
  }

  onAlertClose(action) {
    if (action === 'OK' && this.state.schemaSelected) {
      this.props.deleteSelectedSchema(this.state.schemaSelected);
      let propertyMap = cloneDeep(this.props.propertyMap);
      removeSchemaFromPropertyMap(propertyMap, this.state.schemaSelected.schemaName);
      this.props.updatePropertyMap(propertyMap);
      let detectedProperties = cloneDeep(this.props.detectedProperties);
      remove(detectedProperties, { dataSchemaName: this.state.schemaSelected.schemaName });
      this.props.setDetectedProperties(detectedProperties);
    }
    this.setState({
      openAlertModal: false
    });
  }

  render() {
    let selectedSchemas = [];
    return (
      <div className="schema-step-container">
        <AddSchema operation={this.openSchemaSelectorModal.bind(this)} />
        {
          this.props.selectedSchemas.map((schemaItem) => {
            selectedSchemas.push(schemaItem.schemaName);
            return (<AddSchema title={schemaItem.schemaName} data={schemaItem} key={schemaItem.schemaName} type='ADDED'
              operation={this.performAction.bind(this)} />);
          })
        }
        <SchemaSelectorModal open={this.state.openSchemaModal} onClose={this.onAddSchemaClose} showSchemaSelector={this.state.showSchemaSelector}
          dataProvider={this.state.schemaDP} selectedSchemas ={selectedSchemas} operationType={this.state.operationType} />
        <AlertModal open={this.state.openAlertModal} message={this.state.alertMessage}
          onClose={this.onAlertClose} />
      </div>
    );
  }
}

export default SchemaSelector;

SchemaSelector.propTypes = {
  availableSchemas: PropTypes.array,
  setSelectedSchemas: PropTypes.func,
  updateSelectedSchema: PropTypes.func,
  selectedSchemas: PropTypes.array,
  deleteSelectedSchema: PropTypes.func,
  propertyMap: PropTypes.object,
  updatePropertyMap: PropTypes.func,
  detectedProperties: PropTypes.array,
  setDetectedProperties: PropTypes.func,
};
