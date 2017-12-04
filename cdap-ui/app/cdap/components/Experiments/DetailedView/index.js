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

import PropTypes from 'prop-types';
import React, {Component} from 'react';
import {Provider} from 'react-redux';
import experimentDetailStore from 'components/Experiments/store/experimentDetailStore';
import {getExperimentDetails} from 'components/Experiments/store/ActionCreator';
import ConnectedTopPanel from 'components/Experiments/DetailedView/TopPanel';
import ModelsTableWrapper from 'components/Experiments/DetailedView/ModelsTable';

require('./DetailedView.scss');

export default class ExperimentDetails extends Component {
  static propTypes = {
    match: PropTypes.object,
    location: PropTypes.object
  };
  state = {
    loading: true
  }
  componentWillMount() {
    let {experimentId} = this.props.match.params;
    getExperimentDetails(experimentId);
  }
  render() {
    return (
      <Provider store={experimentDetailStore}>
        <div className="experiment-detailed-view">
          <ConnectedTopPanel />
          <ModelsTableWrapper />
        </div>
      </Provider>
    );
  }
}
