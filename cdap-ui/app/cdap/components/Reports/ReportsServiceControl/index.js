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
import Helmet from 'react-helmet';

const PREFIX = 'features.Reports.ReportsServiceControl';
const ReportsArtifact = 'cdap-program-report';

export default class ReportsServiceControl extends Component {
  static propTypes = {
    onServiceStart: PropTypes.func
  };

  state = {
    loading: false,
    error: null
  };

  enableReports = () => {
    this.setState({
      loading: true
    });

    enableSystemApp({
      shouldStopService: false,
      artifactName: ReportsArtifact,
      api: MyReportsApi,
      i18nPrefix: ''
    }).subscribe(
      this.props.onServiceStart,
      () => {
        this.setState({
          error: 'Unable to start Report service',
          loading: false
        });
      }
    );
  };

  renderEnableBtn = () => {
    return (
      <div className="action-container">
        <BtnWithLoading
          className="btn-primary"
          label={T.translate(`${PREFIX}.enable`)}
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
      <div className="experiments-service-control-error">
        <h5 className="text-danger">
          <IconSVG name="icon-exclamation-triangle" />
          <span>{T.translate(`${PREFIX}.unableToStart`)}</span>
        </h5>
        <p className="text-danger">
          {this.state.error}
        </p>
      </div>
    );
  }

  render() {
    // TODO: this page will still need to go through redesign!

    return (
      <div className="reports-service-control text-xs-center">
        <Helmet title={T.translate('features.Reports.pageTitle')} />
        <br />
        {this.renderEnableBtn()}
        {this.renderError()}
      </div>
    );
  }
}
