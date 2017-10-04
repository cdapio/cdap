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

import PropTypes from 'prop-types';

import React, { Component } from 'react';
import { connect, Provider } from 'react-redux';
import {createSchemaStore} from './SchemaStore';
import SchemaStoreActions from './SchemaStoreActions';

import { Table } from 'reactstrap';
import shortid from 'shortid';
import FieldRow from './FieldRow';
require('./SimpleSchema.scss');

const mapStateToFieldNameProps = (state, ownProps) => {
  return {
    name: state.schema.fields[ownProps.index].name,
    type: state.schema.fields[ownProps.index].type,
    isNullable: state.schema.fields[ownProps.index].isNullable
  };
};
const fieldToActionMap = {
  name: SchemaStoreActions.setFieldName,
  type: SchemaStoreActions.setFieldType,
  isNullable: SchemaStoreActions.setFieldIsNullable,
};
const mapDispatchToFieldNameProps = (dispatch, ownProps) => {
  return {
    onKeyUp: (fieldProp, e) => {
      dispatch({
        type: fieldToActionMap[fieldProp],
        payload: {
          index: ownProps.index,
          keyCode: e.keyCode
        }
      });
      e.preventDefault();
      e.stopPropagation();
      return false;
    },
    onRemove: () => {
      dispatch({
        type: SchemaStoreActions.removeField,
        payload: {index: ownProps.index}
      });
    },
    onChange: (fieldProp, e) => {
      dispatch({
        type: fieldToActionMap[fieldProp],
        payload: {
          index: ownProps.index,
          [fieldProp]: (fieldProp === 'isNullable') ? e.target.checked : e.target.value
        }
      });
      if (fieldProp !== 'isNullable') {
        e.preventDefault();
        e.stopPropagation();
      }
      return false;
    }
  };
};

let FieldRowCopy = connect(
  mapStateToFieldNameProps,
  mapDispatchToFieldNameProps
)(FieldRow);

export default class SimpleSchema extends Component {
  constructor(props) {
    super(props);
    var { schema, onSchemaChange } = props;
    this.state = {
      fields: [...schema.fields]
    };
    this.SchemaStore = createSchemaStore({schema});
    this.subscription = this.SchemaStore.subscribe(() => {
      onSchemaChange(this.SchemaStore.getState().schema);
      this.setState(this.SchemaStore.getState().schema);
    });
  }
  componentWillUnmount() {
    this.subscription();
  }
  shouldComponentUpdate(nextProps) {
    return this.state.fields.length !== nextProps.schema.fields.length;
  }
  componentWillReceiveProps(nextProps) {
    this.setState({
      fields: [...nextProps.schema.fields]
    });
  }
  componentDidUpdate() {
    this.tBody.querySelector('tr:last-child input').focus();
  }
  render() {
    return (
      <Table className="cask-simple-schema">
        <thead>
          <tr>
            <th>Name</th>
            <th>Type</th>
            <th>Null</th>
          </tr>
        </thead>
        <tbody ref={ele => {
          this.tBody = ele;
        }}>
          {
            this.state.fields.map( (field, index) => {
              return (
                <Provider store={this.SchemaStore} key={shortid.generate()}>
                  <FieldRowCopy
                    className="schema-field-row"
                    index={index}
                    />
                </Provider>
              );
            })
          }
        </tbody>
      </Table>
    );
  }
}
SimpleSchema.propTypes = {
  schema: PropTypes.shape({
    type: PropTypes.string,
    name: PropTypes.string,
    fields: PropTypes.arrayOf(PropTypes.shape({
      name: PropTypes.string,
      type: PropTypes.string,
      isNullable: PropTypes.bool
    }))
  }),
  onSchemaChange: PropTypes.func
};
