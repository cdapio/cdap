/*
 * Copyright © 2018 Cask Data, Inc.
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
import React, {Component} from 'react';
import IconSVG from 'components/IconSVG';
import {Input} from 'reactstrap';
import {NUMBER_TYPES} from 'services/global-constants';
import SortableTable from 'components/SortableTable';
import {objectQuery} from 'services/helpers';
import findLast from 'lodash/findLast';

require('./SplitInfoTable.scss');

export default class SplitInfoTable extends Component {
  static propTypes = {
    splitInfo: PropTypes.object,
    onActiveColumnChange: PropTypes.func
  };

  state = {
    collapsed: true,
    splitInfo: this.props.splitInfo,
    search: '',
    selectedTypes: [
      'boolean',
      'double',
      'float',
      'int',
      'long',
      'string'
    ]
  };

  componentWillReceiveProps(nextProps) {
    this.setState({splitInfo: nextProps.splitInfo});
  }

  toggleCollapse = () => {
    this.setState({collapsed: !this.state.collapsed});
  };

  isFieldNumberType = (type) => NUMBER_TYPES.indexOf(type) !== -1;

  CATEGORICAL_FIELD_HEADERS = [
    {
      property: 'name',
      label: 'Column Name'
    },
    {
      property: 'numTotal',
      label: 'Count'
    },
    {
      property: 'numEmpty',
      label: 'Missing'
    },
    {
      property: 'unique',
      label: 'Unique'
    }
  ];

  NUMERICAL_FIELD_HEADERS = [
    {
      property: 'name',
      label: 'Column Name'
    },
    {
      property: 'numTotal',
      label: 'Count'
    },
    {
      property: 'numEmpty',
      label: 'Missing'
    },
    {
      property: 'numZero',
      label: 'Zero'
    },
    {
      property: 'mean',
      label: 'Mean'
    },
    {
      property: 'stddev',
      label: 'Std Dev'
    },
    {
      property: 'min',
      label: 'Min'
    },
    {
      property: 'max',
      label: 'Max'
    }
  ];

  onSearch = (e) => {
    this.setState({search: e.target.value});
  };

  onToggleSelectedType = (e) => {
    if (this.state.selectedTypes.indexOf(e.target.name) !== -1) {
      this.setState({
        selectedTypes: this.state.selectedTypes.filter(type => type !== e.target.name)
      });
    } else {
      this.setState({
        selectedTypes: [...this.state.selectedTypes, e.target.name]
      });
    }
  };

  renderNumericalTableBody = (fields) => {
    return (
      <tbody>
        {
          fields.map(field => {
            return (
              <tr
                key={field.name}
                onClick={this.props.onActiveColumnChange.bind(null, field.name)}
              >
                <td>{field.name}</td>
                <td>{field.numTotal}</td>
                <td>{field.numEmpty}</td>
                <td>{field.numZero}</td>
                <td>{field.mean}</td>
                <td>{field.stddev}</td>
                <td>{field.min}</td>
                <td>{field.max}</td>
              </tr>
            );
          })
        }
      </tbody>
    );
  };

  renderCategoricalTableBody = (fields) => {
    return (
      <tbody>
        {
          fields.map(field => {
            return (
              <tr
                key={field.name}
                onClick={this.props.onActiveColumnChange.bind(null, field.name)}
              >
                <td>{field.name}</td>
                <td>{field.numTotal}</td>
                <td>{field.numEmpty}</td>
                <td>{field.unique}</td>
              </tr>
            );
          })
        }
      </tbody>
    );
  };

  renderNumericalTable = (fields) => {
    return (
      <SortableTable
        entities={fields}
        tableHeaders={this.NUMERICAL_FIELD_HEADERS}
        renderTableBody={this.renderNumericalTableBody}
      />
    );
  };

  renderCategoricalTable = (fields) => {
    return (
      <SortableTable
        entities={fields}
        tableHeaders={this.CATEGORICAL_FIELD_HEADERS}
        renderTableBody={this.renderCategoricalTableBody}
      />
    );
  };

  renderTable = () => {
    if (!objectQuery(this.state.splitInfo, 'schema', 'fields', 'length')) {
      return null;
    }
    const schema = this.state.splitInfo.schema;
    const getFieldType = (field) => {
      let type;
      // TODO: The assumption fields cannot have complex type coming from dataprep wrangler
      // Need to verify this.
      if (Array.isArray(field.type)) {
        type = field.type.filter(t => t !== 'null').pop();
      } else if (typeof field.type === 'string') {
        type = field.type;
      }
      return type;
    };
    const getStats = ({name: fieldName}) => {
      let stat = findLast(this.state.splitInfo.stats, stat => stat.field === fieldName);
      stat = {
        numTotal: objectQuery(stat, 'numTotal', 'total') || '--',
        numNull: objectQuery(stat, 'numNull', 'total') || '--',
        numEmpty: objectQuery(stat, 'numEmpty', 'total') || '--',
        unique: objectQuery(stat, 'unique', 'total') || '--',
        numZero: objectQuery(stat, 'numZero', 'total') || '--',
        numPositive: objectQuery(stat, 'numPositive', 'total') || '--',
        numNegative: objectQuery(stat, 'numNegative', 'total') || '--',
        min: objectQuery(stat, 'min', 'total') || '--',
        max: objectQuery(stat, 'max', 'total') || '--',
        stddev: objectQuery(stat, 'stddev', 'total') || '--',
        mean: objectQuery(stat, 'mean', 'total') || '--'
      };
      return {
        name: fieldName,
        ...stat
      };
    };
    const searchMatchFilter = (field => {
      if (this.state.search.length) {
        return field.name.indexOf(this.state.search) !== -1 ? true : false;
      }
      return field;
    });
    const typeMatchFilter = (field => this.state.selectedTypes.indexOf(getFieldType(field)) !== -1);
    const categoricalFields = schema.fields
      .filter((field) => typeMatchFilter(field) && searchMatchFilter(field) && !this.isFieldNumberType(getFieldType(field)))
      .map(getStats.bind(this));
    const numericalFields = schema.fields
      .filter((field) => typeMatchFilter(field) && searchMatchFilter(field) && this.isFieldNumberType(getFieldType(field)))
      .map(getStats.bind(this));
    const countOfType = (type) => schema.fields.filter(field => getFieldType(field) === type).length;
    return (
      <div className="split-info-table-container">
        <div className="split-table-search">
        <div className="filter-container">
          <span> Data Type: </span>
          <span>
            <Input
              type="checkbox"
              checked={this.state.selectedTypes.indexOf('boolean') !== -1}
              onChange={this.onToggleSelectedType}
              name="boolean"
            />
            <span> Boolean ({countOfType('boolean')}) </span>
          </span>
          <span>
            <Input
              type="checkbox"
              checked={this.state.selectedTypes.indexOf('double') !== -1}
              onChange={this.onToggleSelectedType}
              name="double"
            />
            <span> Double ({countOfType('double')}) </span>
          </span>
          <span>
            <Input
              type="checkbox"
              checked={this.state.selectedTypes.indexOf('float') !== -1}
              onChange={this.onToggleSelectedType}
              name="float"
            />
            <span> Float ({countOfType('float')}) </span>
          </span>
          <span>
            <Input
              type="checkbox"
              checked={this.state.selectedTypes.indexOf('int') !== -1}
              onChange={this.onToggleSelectedType}
              name="int"
            />
            <span> Integer ({countOfType('int')}) </span>
          </span>
          <span>
            <Input
              type="checkbox"
              checked={this.state.selectedTypes.indexOf('long') !== -1}
              onChange={this.onToggleSelectedType}
              name="long"
            />
            <span> Long ({countOfType('long')}) </span>
          </span>
          <span>
            <Input
              type="checkbox"
              checked={this.state.selectedTypes.indexOf('string') !== -1}
              onChange={this.onToggleSelectedType}
              name="string"
            />
            <span> String ({countOfType('string')}) </span>
          </span>
        </div>
        <Input
          className="table-field-search"
          placeholder="Search Column name"
          onChange={this.onSearch}
        />
        </div>
        <div className="split-info-numerical-table">
          <div className="split-table-header">Numerical Data</div>
          {this.renderNumericalTable(numericalFields)}
        </div>
        <div className="split-info-categorical-table">
          <div className="split-table-header">Categorical Data</div>
          {this.renderCategoricalTable(categoricalFields)}
        </div>
      </div>
    );
  };

  render() {
    return (
      <div className="split-info-table">
        <div className="split-info-collapsable-section" onClick={this.toggleCollapse}>
          {
            this.state.collapsed ? <IconSVG name="icon-caret-right" /> : <IconSVG name="icon-caret-down" />
          }
          <span> Select a row to inspect how Test Data is compared to the Training Data </span>
        </div>
        {
          !this.state.collapsed ?
            <div className="split-info-table-section">
              {this.renderTable()}
            </div>
          :
            null
        }
      </div>
    );
  }
}
