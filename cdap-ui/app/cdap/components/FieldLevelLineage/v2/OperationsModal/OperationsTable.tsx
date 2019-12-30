/*
 * Copyright © 2019 Cask Data, Inc.
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

import React, { Component } from 'react';
import { FllContext } from 'components/FieldLevelLineage/v2/Context/FllContext';
import classnames from 'classnames';
import T from 'i18n-react';

const PREFIX = 'features.FieldLevelLineage.OperationsModal.Table';

interface IOpsTableState {
  activeOrigin?: string;
  activeField?: { operation?: string; name?: string };
}

export default class OperationsTable extends Component<{}, IOpsTableState> {
  public state = {
    activeOrigin: null,
    activeField: { operation: null, name: null },
  };

  public componentWillReceiveProps() {
    this.setState({
      activeOrigin: null,
      activeField: { operation: null, name: null },
    });
  }

  private handleInputClick(field, operation) {
    this.setState({
      activeOrigin: field.origin,
      activeField: {
        operation: operation.name,
        name: field.name,
      },
    });
  }

  private joinEndpoints(endpoints) {
    if (!endpoints || !endpoints.endPoint) {
      return '--';
    }

    return endpoints.endPoint.name;
  }

  private renderInput(operation) {
    return this.joinEndpoints(operation.inputs);
  }

  private renderOutput(operation) {
    return this.joinEndpoints(operation.outputs);
  }

  private renderInputFields(operation) {
    const fields = operation.inputs.fields;
    if (!fields) {
      return '--';
    }

    return fields.map((field, i) => {
      const activeField = this.state.activeField;
      const isSelected =
        activeField.operation === operation.name &&
        activeField.name === field.name &&
        this.state.activeOrigin === field.origin;

      return (
        <span key={`${field.name}-${i}`}>
          <span
            className={classnames('input-field', { selected: isSelected })}
            onClick={this.handleInputClick.bind(this, field, operation)}
          >
            [{field.name}]
          </span>
          {i !== fields.length - 1 ? ', ' : null}
        </span>
      );
    });
  }

  private renderOutputFields(operation) {
    if (!operation.outputs.fields) {
      return '--';
    }

    return operation.outputs.fields.join(', ');
  }

  private renderHeader() {
    const headers = ['input', 'inputFields', 'pluginName', 'description', 'outputFields', 'output'];

    return (
      <div className="grid-header">
        <div className="grid-row">
          <div />
          {headers.map((head) => {
            return <div key={head}>{T.translate(`${PREFIX}.${head}`)}</div>;
          })}
        </div>
      </div>
    );
  }

  private renderBody(operations) {
    return (
      <div className="grid-body">
        {operations.map((operation, i) => {
          let description = null;
          const pluginName = operation.name.split('.')[0];

          if (operation.description) {
            const descriptionSplit = operation.description.split('\n');
            description = descriptionSplit.map((line, index) => {
              return (
                <React.Fragment key={index}>
                  <span>{line}</span>
                  {descriptionSplit.length - 1 !== index ? <br /> : null}
                </React.Fragment>
              );
            });
          }

          return (
            <div
              key={`${operation.name}-${i}`}
              className={classnames('grid-row', {
                active: operation.name === this.state.activeOrigin,
              })}
            >
              <div>{i + 1}</div>
              <div>{this.renderInput(operation)}</div>
              <div>{this.renderInputFields(operation)}</div>
              <div title={pluginName}>{pluginName}</div>
              <div title={operation.description}>{description}</div>
              <div>{this.renderOutputFields(operation)}</div>
              <div>{this.renderOutput(operation)}</div>
            </div>
          );
        })}
      </div>
    );
  }

  public render() {
    const { operations, activeOpsIndex } = this.context;
    return (
      <div className="grid-wrapper">
        <div className="grid grid-container grid-compact">
          {this.renderHeader()}
          {this.renderBody(operations[activeOpsIndex].operations)}
        </div>
      </div>
    );
  }
}

OperationsTable.contextType = FllContext;
