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

import React, { Component } from 'react';
import ee from 'event-emitter';
import T from 'i18n-react';
import PropTypes from 'prop-types';
import classnames from 'classnames';
import DataPrepStore from 'components/DataPrep/store';

const PREFIX = 'features.DataPrep.DataPrepTable.DataType';

export default class DataType extends Component {
  constructor(props) {
    super(props);

    this.state = {
      highlightedDataType: null,
    };

    this.eventEmitter = ee(ee);
    this.eventEmitter.on('DATAPREP_DATA_TYPE_CHANGED', this.changeDataTypeColor);
  }

  componentWillUnmount() {
    this.eventEmitter.off('DATAPREP_DATA_TYPE_CHANGED', this.changeDataTypeColor);
  }

  changeDataTypeColor = (columnName) => {
    this.setState({ highlightedDataType: columnName }, () => {
      setTimeout(() => {
        this.setState({ highlightedDataType: null });
      }, 3000);
    });
  };

  render() {
    let types = DataPrepStore.getState().dataprep.types;
    return (
      <div
        className={classnames('col-type', {
          'data-type-updated': this.state.highlightedDataType === this.props.columnName,
        })}
      >
        {types[this.props.columnName] || T.translate(`${PREFIX}.unknown`)}
      </div>
    );
  }
}

DataType.propTypes = {
  columnName: PropTypes.string,
};
