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
import { MyMarketApi } from 'api/market';
import MarketStore from 'components/Market/store/market-store';
import T from 'i18n-react';
require('./LicenseStep.scss');

export default class LicenseStep extends Component {
  constructor(props) {
    super(props);
    this.state = {
      license: null,
    };
  }
  componentWillMount() {
    let { entityName, entityVersion, licenseFileName } = this.props;
    const marketHost = MarketStore.getState().selectedMarketHost;
    let params = {
      entityName,
      entityVersion,
      marketHost,
      filename: licenseFileName,
    };
    MyMarketApi.getSampleData(params).subscribe((license) => {
      this.setState({
        license,
      });
    });
  }
  render() {
    if (!this.state.license) {
      return (
        <div className="spinner-container">
          <div className="fa fa-spin fa-spinner fa-3x" />
        </div>
      );
    }
    return (
      <div className="license-container">
        <h2>{T.translate('features.Wizard.licenseStep.termsandconditions')}</h2>
        <pre className="license-text">{this.state.license}</pre>
        <div>
          <div className="btn btn-primary agree-btn" onClick={this.props.onAgree.bind(this)}>
            {T.translate('features.Wizard.licenseStep.agreeAndActionBtnLabel')}
          </div>
          <div className="back-to-cdap-link" onClick={this.props.onReject.bind(this, false)}>
            {T.translate('features.Wizard.licenseStep.backToCaskBtnLabel')}
          </div>
        </div>
      </div>
    );
  }
}
LicenseStep.propTypes = {
  entityName: PropTypes.string,
  entityVersion: PropTypes.string,
  licenseFileName: PropTypes.string,
  onReject: PropTypes.func.required,
  onAgree: PropTypes.func.required,
};
