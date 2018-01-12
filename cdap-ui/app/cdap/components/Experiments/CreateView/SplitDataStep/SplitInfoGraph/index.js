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
import GroupedBarChart from 'components/GroupedBarChart';

export default class SplitInfoGraph extends Component {
  static propTypes = {
    splitInfo: PropTypes.object.isRequired,
    activeColumn: PropTypes.string.isRequired
  };

  state = {
    splitInfo: this.props.splitInfo,
    activeColumn: this.props.activeColumn
  };

  customEncoding = {
    "x": {
      "field": "type", "type": "nominal",
      "axis": null
    },
    "column": {
      "field": "name", "type": "ordinal", "header": {"title": "values"}
    },
    "y": {
      "field": "count", "type": "quantitative",
      "axis": {"title": "Count (Percent)", "grid": false}
    },
  };

  componentWillReceiveProps(nextProps) {
    this.setState({
      activeColumn: nextProps.activeColumn,
      splitInfo: nextProps.splitInfo
    });
  }

  getFormattedData = () => {
    const matchingField = this.state.splitInfo.stats
      .filter(stat => stat.field === this.state.activeColumn)
      .pop();
    if (!matchingField) {
      return [];
    }
    const getTotalValues = (dataType = 'train') => {
      const total = matchingField.numTotal[dataType];
      const nulls = matchingField.numNull[dataType];
      return total - nulls;
    };
    return matchingField
      .histo
      .reduce(
        (prev, curr) => {
          prev = prev || [];
          return [
            ...prev,
            {
              name: curr.bin,
              count: curr.count.train / getTotalValues('train'),
              type: 'train'
            },
            {
              name: curr.bin,
              count: curr.count.test / getTotalValues('test'),
              type: 'test'
            }
          ];
        }, []
      );
  };

  render () {
    if (!this.state.splitInfo.stats || !this.state.activeColumn) {
      return null;
    }
    return (
      <GroupedBarChart
        customEncoding={this.customEncoding}
        data={this.getFormattedData()}
        width={(dimension, data) => {
          if (!data.length) {
            return (dimension.width - 260);
          }
          return ((dimension.width - 260) / (data.length / 2));
        }}
        heightOffset={80}
        tooltipOptions={{
          showAllFields: false,
          fields: [
            {
              field: 'type',
              title: 'Type'
            },
            {
              field: 'count',
              title: 'Percentage',
              format: '.0%',
              formatType: 'number'
            }
          ]
        }}
      />
    );
  }
}
