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
import PropTypes from 'prop-types';
import { SUCCEEDED, FAILED, DEPLOYED, RUNNING } from 'components/FeatureUI/config';

require('./StatusRenderer.scss');

class StatusRenderer extends React.Component {
  constructor(props) {
    super(props);
  }
  refresh() {
    return true;
  }

  render() {
    return <div>
      <span className = {this.getStatusClass(this.props.value)}></span>
      {this.props.value}
    </div>;
  }
  getStatusClass(status) {
    let className = "fa fa-circle status-padding";
    switch (status) {
      case SUCCEEDED:
        className += " status-success";
        break;
      case FAILED:
        className += " status-failed";
        break;
      case DEPLOYED:
        className += " status-deployed";
        break;
      case RUNNING:
        className += " status-running";
        break;
    }
    return className;
  }
}
export default StatusRenderer;
StatusRenderer.propTypes = {
  value: PropTypes.string
};
