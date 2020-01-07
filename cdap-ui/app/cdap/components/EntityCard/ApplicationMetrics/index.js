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
import { humanReadableNumber } from 'services/helpers';
import T from 'i18n-react';

export default class ApplicationMetrics extends Component {
  constructor(props) {
    super(props);
  }

  render() {
    const loading = <span className="fa fa-spin fa-spinner" />;

    return (
      <div className="metrics-container">
        <div className="metric-item">
          <p className="metric-header">{T.translate('commons.entity.metrics.programs')}</p>
          <p>
            {!this.props.extraInfo
              ? loading
              : humanReadableNumber(this.props.extraInfo.numPrograms)}
          </p>
        </div>
        <div className="metric-item">
          <p className="metric-header">{T.translate('commons.entity.metrics.running')}</p>
          <p>
            {!this.props.extraInfo ? loading : humanReadableNumber(this.props.extraInfo.running)}
          </p>
        </div>
        <div className="metric-item">
          <p className="metric-header">{T.translate('commons.entity.metrics.failed')}</p>
          <p>
            {!this.props.extraInfo ? loading : humanReadableNumber(this.props.extraInfo.failed)}
          </p>
        </div>
      </div>
    );
  }
}

ApplicationMetrics.propTypes = {
  entity: PropTypes.object,
  extraInfo: PropTypes.object,
};
