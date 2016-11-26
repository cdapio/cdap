/*
 * Copyright © 2016 Cask Data, Inc.
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

import React, {Component} from 'react';
import {Provider} from 'react-redux';
require('./SchemaEditor.less');
import {getParsedSchema} from 'components/SchemaEditor/SchemaHelpers';
import RecordSchemaRow from 'components/SchemaEditor/RecordSchemaRow';
import SchemaStore from 'components/SchemaEditor/SchemaStore';

export default class SchemaEditor extends Component {
  constructor(props) {
    super(props);
    let state = SchemaStore.getState();
    let rows;
    try {
      rows = avsc.parse(state.schema, { wrapUnions: true });
    } catch(e) {
      console.log('Error parsing schema: ', e);
    }
    this.state = {
      parsedRows: rows,
      rawSchema: state.schema
    };
    const updateRowsAndDisplayFields = () => {
      let state = SchemaStore.getState();
      let rows;
      try {
        rows = getParsedSchema(state.schema);
      } catch(e) {
        return;
      }
      this.setState({
        parsedRows: rows,
        rawSchema: state.schema
      });
    };
    SchemaStore.subscribe(updateRowsAndDisplayFields.bind(this));
  }
  shouldComponentUpdate(nextProps, nextState) {
    return nextState.rawSchema.fields.length !== this.state.rawSchema.fields.length;
  }
  onChange(schema) {
    SchemaStore.dispatch({
      type: 'FIELD_UPDATE',
      payload: {
        schema
      }
    });
  }
  render() {
    return (
      <Provider store={SchemaStore}>
        <div className="schema-editor">
          <div className="schema-header">
            <div className="field-name">
              Name
            </div>
            <div className="field-type">
              Type
            </div>
            <div className="field-isnull">
              Null
            </div>
          </div>
          <div className="schema-body">
            <RecordSchemaRow
              row={this.state.parsedRows}
              onChange={this.onChange.bind(this)}
            />
          </div>
        </div>
      </Provider>
    );
  }
}
