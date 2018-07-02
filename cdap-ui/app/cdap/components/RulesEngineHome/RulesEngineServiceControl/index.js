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
import MyRuleEngineApi from 'api/rulesengine';
import enableSystemApp from 'services/ServiceEnablerUtilities';
import LoadingSVG from 'components/LoadingSVG';
import T from 'i18n-react';
import IconSVG from 'components/IconSVG';

require('./RulesEngineServiceControl.scss');
const PREFIX = 'features.RulesEngine.RulesEngineServiceControl';
const RulesEngineArtifact = 'dre-service';

export default class RulesEngineServiceControl extends Component {

  static propTypes = {
    onServiceStart: PropTypes.func
  };

  state = {
    loading: false,
    error: null
  };

  enableRulesEngine = () => {
    this.setState({
      loading: true
    });
    enableSystemApp({
      shouldStopService: false,
      artifactName: RulesEngineArtifact,
      api: MyRuleEngineApi,
      i18nPrefix: ''
    })
      .subscribe(
        this.props.onServiceStart,
        () => {
          this.setState({
            error: T.translate(`${PREFIX}.errorMessage`),
            loading: false
          });
        }
      );
  }

  renderError = () => {
    if (!this.state.error) {
      return null;
    }
    return (
      <div className="rules-engine-service-control-error">
        <h5 className="text-danger">
          <IconSVG name="icon-exclamation-triangle" />
          <span>{T.translate(`${PREFIX}.errorTitle`)}</span>
        </h5>
        <p className="text-danger">
          {this.state.error}
        </p>
      </div>
    );
  }

  renderEnableBtn = () => {
    return (
      <div className="action-container">
        <button
          className="btn btn-primary"
          onClick={this.enableRulesEngine}
          disabled={this.state.loading}
        >
          {
            this.state.loading ?
              <LoadingSVG />
            :
              null
          }
          <span className="btn-label">{T.translate(`${PREFIX}.enableBtnLabel`)}</span>
        </button>
      </div>
    );
  };

  render() {
    return (
      <div className="rules-engine-service-control">
        <div className="image-containers">
          <img className="img-thumbnail" src="/cdap_assets/img/RulesEngine_preview_1.png" />
          <img className="img-thumbnail" src="/cdap_assets/img/RulesEngine_preview_2.png" />
        </div>
        <div className="text-container">
          <h2> {T.translate(`${PREFIX}.title`)} </h2>
          {this.renderEnableBtn()}
          {this.renderError()}
          <p>
            {T.translate(`${PREFIX}.description`)}
          </p>
          <div className="rules-engine-benefit">
            {T.translate(`${PREFIX}.benefits.title`)}

            <ul>
              <li>
                <span className="fa fa-laptop" />
                <span>{T.translate(`${PREFIX}.benefits.b1`)}</span>
              </li>
              <li>
                <IconSVG name="icon-edit" />
                <span>{T.translate(`${PREFIX}.benefits.b2`)}</span>
              </li>
              <li>
                <IconSVG name="icon-cogs" />
                <span>{T.translate(`${PREFIX}.benefits.b3`)}</span>
              </li>
              <li>
                <IconSVG name="icon-arrows-alt" />
                <span>{T.translate(`${PREFIX}.benefits.b4`)}</span>
              </li>
              <li>
                <span className="fa fa-university" />
                <span>{T.translate(`${PREFIX}.benefits.b5`)}</span>
              </li>
            </ul>
          </div>
        </div>
      </div>
    );
  }
}
