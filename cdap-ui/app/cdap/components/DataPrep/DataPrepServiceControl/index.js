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

import React, { Component, PropTypes } from 'react';
import enableDataPreparationService from 'components/DataPrep/DataPrepServiceControl/ServiceEnablerUtilities';
import T from 'i18n-react';
import classnames from 'classnames';
import {objectQuery} from 'services/helpers';
import MyDataPrepApi from 'api/dataprep';
import {i18nPrefix, MIN_DATAPREP_VERSION, artifactName} from 'components/DataPrep';

require('./DataPrepServiceControl.scss');

const PREFIX = `features.DataPrepServiceControl`;

export default class DataPrepServiceControl extends Component {
  constructor(props) {
    super(props);

    this.state = {
      loading: false,
      error: null,
      extendedMessage: null
    };

    this.enableService = this.enableService.bind(this);
  }

  componentWillUnmount() {
    if (this.servicePoll && this.servicePoll.dispose) {
      this.servicePoll.dispose();
    }
  }

  enableService() {
    this.setState({loading: true});
    enableDataPreparationService({
      shouldStopService: false,
      artifactName,
      api: MyDataPrepApi,
      i18nPrefix,
      MIN_VERSION: MIN_DATAPREP_VERSION
    })
      .subscribe(() => {
        this.props.onServiceStart();
      }, (err) => {
        this.setState({
          error: err.error,
          extendedMessage: objectQuery(err, 'extendedMessage', 'response'),
          loading: false
        });
      });
  }

  renderError() {
    if (!this.state.error) { return null; }

    return (
      <div className="dataprep-service-control-error">
        <h5 className="text-danger">
          {this.state.error}
        </h5>
        <p className="text-danger">
          {this.state.extendedMessage}
        </p>
      </div>
    );
  }

  render() {
    return (
      <div className={classnames("dataprep-container dataprep-service-control", {
        'error': this.state.error
      })}>
        <div className="service-control-container">
          <div className="image-container">
            <img src="/cdap_assets/img/DataPrep_preview1.png" className="img-thumbnail" />
            <img src="/cdap_assets/img/DataPrep_preview2.png" className="img-thumbnail" />
          </div>
          <div className="text-container">
            <div className="description-container">
              <h2 className="text-xs-left">
                {T.translate(`${PREFIX}.title`)}
              </h2>
              <p>
                {T.translate(`${PREFIX}.description`)}
              </p>
              <ul className="dataprep-checklist">
                <li>{T.translate(`${PREFIX}.list.1`)}</li>
                <li>{T.translate(`${PREFIX}.list.2`)}</li>
                <li>{T.translate(`${PREFIX}.list.3`)}</li>
                <li>{T.translate(`${PREFIX}.list.4`)}</li>
              </ul>
              <div className="text-xs-left">
                <button
                  className="btn btn-primary"
                  onClick={this.enableService}
                  disabled={this.state.loading}
                >
                  {
                    !this.state.loading ? T.translate(`${PREFIX}.btnLabel`) : (
                      <span>
                        <span className="fa fa-spin fa-spinner" /> {T.translate(`${PREFIX}.btnLoadingLabel`)}
                      </span>
                    )
                  }
                </button>
              </div>
              {this.renderError()}
            </div>
          </div>
        </div>
      </div>
    );
  }
}
DataPrepServiceControl.propTypes = {
  onServiceStart: PropTypes.func
};
