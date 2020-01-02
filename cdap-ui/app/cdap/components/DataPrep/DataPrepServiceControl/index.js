/*
 * Copyright © 2017-2018 Cask Data, Inc.
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
import enableSystemApp from 'services/ServiceEnablerUtilities';
import T from 'i18n-react';
import classnames from 'classnames';
import MyDataPrepApi from 'api/dataprep';
import { i18nPrefix, MIN_DATAPREP_VERSION, artifactName } from 'components/DataPrep';
import isObject from 'lodash/isObject';
import { Theme } from 'services/ThemeHelper';
import { objectQuery } from 'services/helpers';

require('./DataPrepServiceControl.scss');

const PREFIX = `features.DataPrepServiceControl`;

export default class DataPrepServiceControl extends Component {
  state = {
    loading: false,
    error: null,
    extendedMessage: null,
  };

  enableService = () => {
    this.setState({ loading: true });
    const featureName = Theme.featureNames.dataPrep;
    enableSystemApp({
      shouldStopService: false,
      artifactName,
      api: MyDataPrepApi,
      i18nPrefix,
      MIN_VERSION: MIN_DATAPREP_VERSION,
      featureName,
    }).subscribe(
      () => {
        this.props.onServiceStart();
      },
      (err) => {
        let extendedMessage = isObject(err.extendedMessage)
          ? err.extendedMessage.response || err.extendedMessage.message
          : err.extendedMessage;
        let statusCode = objectQuery(err, 'extendedMessage', 'statusCode');
        // 409 = conflict in status, meaning when we are trying to start the app
        // when it is already running.
        if (
          statusCode === 409 &&
          typeof extendedMessage === 'string' &&
          extendedMessage.indexOf('already running') !== -1
        ) {
          return this.props.onServiceStart();
        }
        this.setState({
          error: err.error,
          extendedMessage,
          loading: false,
        });
      }
    );
  };

  renderError() {
    if (!this.state.error) {
      return null;
    }

    return (
      <div className="dataprep-service-control-error">
        <h5 className="text-danger">{this.state.error}</h5>
        <p className="text-danger">{this.state.extendedMessage}</p>
      </div>
    );
  }

  render() {
    const featureName = Theme.featureNames.dataPrep;

    return (
      <div
        className={classnames('dataprep-container dataprep-service-control', {
          error: this.state.error,
        })}
      >
        <div className="service-control-container">
          <div className="image-container">
            <img src="/cdap_assets/img/DataPrep_preview1.png" className="img-thumbnail" />
            <img src="/cdap_assets/img/DataPrep_preview2.png" className="img-thumbnail" />
          </div>
          <div className="text-container">
            <div className="description-container">
              <h2 className="text-left">{T.translate(`${PREFIX}.title`, { featureName })}</h2>
              <div className="text-left action-container">
                <button
                  className="btn btn-primary"
                  onClick={this.enableService}
                  disabled={this.state.loading}
                >
                  {!this.state.loading ? (
                    T.translate(`${PREFIX}.btnLabel`, { featureName })
                  ) : (
                    <span>
                      <span className="fa fa-spin fa-spinner" />{' '}
                      {T.translate(`${PREFIX}.btnLoadingLabel`)}
                    </span>
                  )}
                </button>
              </div>
              {this.renderError()}
              <p>{T.translate(`${PREFIX}.description`, { featureName })}</p>
              <ul className="dataprep-checklist">
                <li>{T.translate(`${PREFIX}.list.1`)}</li>
                <li>{T.translate(`${PREFIX}.list.2`)}</li>
                <li>{T.translate(`${PREFIX}.list.3`)}</li>
                <li>{T.translate(`${PREFIX}.list.4`)}</li>
              </ul>
            </div>
          </div>
        </div>
      </div>
    );
  }
}
DataPrepServiceControl.propTypes = {
  onServiceStart: PropTypes.func,
};
