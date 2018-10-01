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

import React, { Component } from 'react';
import PropTypes from 'prop-types';
import enableSystemApp from 'services/ServiceEnablerUtilities';
import {MyReportsApi} from 'api/reports';
import IconSVG from 'components/IconSVG';
import BtnWithLoading from 'components/BtnWithLoading';
import T from 'i18n-react';
import {isSpark2Available} from 'services/CDAPComponentsVersions';
import isObject from 'lodash/isObject';
import Helmet from 'react-helmet';
import {Theme} from 'services/ThemeHelper';

require('./ReportsServiceControl.scss');

const PREFIX = 'features.Reports.ReportsServiceControl';
const ReportsArtifact = 'cdap-program-report';

export default class ReportsServiceControl extends Component {
  static propTypes = {
    onServiceStart: PropTypes.func
  };

  state = {
    loading: false,
    disabled: false,
    checkingForSpark2: true,
    error: null,
    extendedError: null
  };

  componentDidMount() {
    isSpark2Available()
      .subscribe(
        isAvailable => this.setState({
          disabled: !isAvailable,
          checkingForSpark2: false
        })
      );
  }

  enableReports = () => {
    this.setState({
      loading: true
    });

    const featureName = Theme.featureNames.reports;

    enableSystemApp({
      shouldStopService: false,
      artifactName: ReportsArtifact,
      api: MyReportsApi,
      i18nPrefix: PREFIX,
      featureName
    }).subscribe(
      this.props.onServiceStart,
      (err) => {
        let extendedMessage = isObject(err.extendedMessage) ?
          err.extendedMessage.response || err.extendedMessage.message
        :
          err.extendedMessage;
        this.setState({
          error: err.error,
          extendedError: extendedMessage,
          loading: false
        });
      }
    );
  };

  renderEnableBtn = () => {
    const featureName = Theme.featureNames.reports;
    if (this.state.checkingForSpark2) {
      return (
        <div className="action-container service-disabled">
          <IconSVG name="icon-spinner" className="fa-spin" />
          <div className="text-primary">
            {T.translate(`${PREFIX}.environmentCheckMessage`, { featureName })}
          </div>
        </div>
      );
    }

    if (this.state.disabled) {
      return (
        <div className="action-container service-disabled">
          <IconSVG name="icon-exclamation-triangle" className="text-danger" />
          <div className="text-danger">
            {T.translate(`${PREFIX}.serviceDisabledMessage`, { featureName })}
          </div>
        </div>
      );
    }
    return (
      <div className="action-container">
        <BtnWithLoading
          className="btn-primary"
          label={T.translate(`${PREFIX}.enable`, { featureName })}
          loading={this.state.loading}
          onClick={this.enableReports}
        />
      </div>
    );
  };

  renderError = () => {
    if (!this.state.error) {
      return null;
    }
    return (
      <div className="reports-service-control-error">
        <h5 className="text-danger">
          <IconSVG name="icon-exclamation-triangle" />
          <span>{this.state.error}</span>
        </h5>
        <p className="text-danger">
          {this.state.extendedError}
        </p>
      </div>
    );
  }

  render() {
    const featureName = Theme.featureNames.reports;

    return (
      <div className="reports-service-control">
        <Helmet title={T.translate('features.Reports.pageTitle', {
          productName: Theme.productName,
          featureName
        })} />
        <div className="image-containers">
          <img className="img-thumbnail" src="/cdap_assets/img/Reports_preview1.png" />
          <img className="img-thumbnail" src="/cdap_assets/img/Reports_preview2.png" />
        </div>
        <div className="text-container">
          <h2> {T.translate(`${PREFIX}.title`, { featureName })} </h2>
          {this.renderEnableBtn()}
          {this.renderError()}
          <p>
            {T.translate(`${PREFIX}.description`, { featureName })}
          </p>
          <div className="reports-benefit">
            {T.translate(`${PREFIX}.Benefits.title`, { featureName })}

            <ul>
              <li>
                <span>{T.translate(`${PREFIX}.Benefits.b1`)}</span>
              </li>
              <li>
                <span>{T.translate(`${PREFIX}.Benefits.b2`)}</span>
              </li>
              <li>
                <span>{T.translate(`${PREFIX}.Benefits.b3`)}</span>
              </li>
              <li>
                <span>{T.translate(`${PREFIX}.Benefits.b4`)}</span>
              </li>
              <li>
                <span>{T.translate(`${PREFIX}.Benefits.b5`)}</span>
              </li>
              <li>
                <span>{T.translate(`${PREFIX}.Benefits.b6`)}</span>
              </li>
            </ul>
          </div>
        </div>
      </div>
    );
  }
}
