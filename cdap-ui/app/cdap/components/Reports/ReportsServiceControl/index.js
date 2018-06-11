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
import enableDataPreparationService from 'components/DataPrep/DataPrepServiceControl/ServiceEnablerUtilities';
import {MyArtifactApi} from 'api/artifact';
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
    error: null,
    artifactNotAvailable: false,
    showEnableButton: false
  };

  componentDidMount() {
    this.checkIfReportsIsAvailable();
  }

  checkIfReportsIsAvailable = () => {
    // The artifact will be in the system namespace, so we are just checking the default namespace
    // since default namespace will always be there.
    let params = {
      namespace: 'default',
      scope: 'SYSTEM'
    };

    MyArtifactApi.list(params)
      .subscribe(
        (artifacts) => {
          let isArtifactPresent = artifacts.find(artifact => artifact.name === ReportsArtifact);
          if (!isArtifactPresent) {
            this.setState({
              artifactNotAvailable: true
            });
          } else {
            this.setState({
              showEnableButton: true
            });
          }
        }
      );
  };

  enableReports = () => {
    this.setState({
      loading: true
    });

    enableDataPreparationService({
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

  renderAvailableOrEnableBtn = () => {
    if (!this.state.artifactNotAvailable && !this.state.showEnableButton) {
      return (
        <span>{T.translate(`${PREFIX}.checking`)}</span>
      );
    }
    if (this.state.artifactNotAvailable) {
      return (
        <div className="action-container">
          <span className="mail-to-link">
            {T.translate(`${PREFIX}.contact`)}
          </span>
        </div>
      );
    }
    if (this.state.showEnableButton) {
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
    }
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
        {this.renderAvailableOrEnableBtn()}
        {this.renderError()}
      </div>
    );
  }
}
