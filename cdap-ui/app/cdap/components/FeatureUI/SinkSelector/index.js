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
import cloneDeep from 'lodash/cloneDeep';
import PropTypes from 'prop-types';

require('./SinkSelector.scss');

class SinkSelector extends React.Component {
  availableSinks;
  sinkConfigurations;
  constructor(props) {
    super(props);
    this.configPropList = [];
  }

  componentDidMount() {
    this.availableSinks = cloneDeep(this.props.availableSinks);
    this.sinkConfigurations = cloneDeep(this.props.sinkConfigurations);
  }


  updateConfiguration() {
  }

  render() {
    return (
      <div className = 'sink-step-container'>
      </div>
    );
  }
}
export default SinkSelector;
SinkSelector.propTypes = {
  availableSinks: PropTypes.array,
  sinkConfigurations: PropTypes.any
};
