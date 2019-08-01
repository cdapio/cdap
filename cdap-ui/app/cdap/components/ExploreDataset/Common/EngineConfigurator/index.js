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
import NameValueList from '../NameValueList';
import cloneDeep from 'lodash/cloneDeep';
import isNil from 'lodash/isNil';
import findIndex from 'lodash/findIndex';
import PropTypes from 'prop-types';

require('./EngineConfigurator.scss');

class EngineConfigurator extends React.Component {
  configPropList;
  constructor(props) {
    super(props);
    this.configPropList = [];
  }

  componentDidMount() {
    this.configPropList = cloneDeep(this.props.engineConfigurations);
  }

  addConfiguration(nameValue) {
    this.configPropList.push(nameValue);
    this.props.updateEngineConfigurations(this.configPropList);
  }

  updateConfiguration(nameValue) {
    if (!isNil(nameValue)) {
      const index = findIndex(this.configPropList, { name: nameValue.name });
      if (index >= 0) {
        this.configPropList[index] = nameValue;
        this.props.updateEngineConfigurations(this.configPropList);
      }
    }

  }

  render() {
    return (
      <div className='configuration-step-container'>
        <NameValueList dataProvider={this.props.engineConfigurations}
          updateNameValue={this.updateConfiguration.bind(this)}
          addNameValue={this.addConfiguration.bind(this)} />
      </div>
    );
  }
}
export default EngineConfigurator;
EngineConfigurator.propTypes = {
  engineConfigurations: PropTypes.array,
  updateEngineConfigurations: PropTypes.func
};
