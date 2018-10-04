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
import isObject from 'lodash/isObject';
import {Theme} from 'services/ThemeHelper';

require('./RulesEngineServiceControl.scss');
const PREFIX = 'features.RulesEngine.RulesEngineServiceControl';
const RulesEngineArtifact = 'dre-service';

export default class RulesEngineServiceControl extends Component {

  static propTypes = {
    onServiceStart: PropTypes.func
  };

  state = {
    loading: false,
    error: null,
    extendedError: null
  };

  enableRulesEngine = () => {
    this.setState({
      loading: true
    });

    const featureName = Theme.featureNames.rulesEngine;
    enableSystemApp({
      shouldStopService: false,
      artifactName: RulesEngineArtifact,
      api: MyRuleEngineApi,
      i18nPrefix: PREFIX,
      featureName
    })
      .subscribe(
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
  }

  renderError = () => {
    if (!this.state.error) {
      return null;
    }
    return (
      <div className="rules-engine-service-control-error">
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

  renderEnableBtn = () => {
    const featureName = Theme.featureNames.rulesEngine;
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
          <span className="btn-label">{T.translate(`${PREFIX}.enableBtnLabel`, { featureName })}</span>
        </button>
      </div>
    );
  };

  render() {
    const featureName = Theme.featureNames.rulesEngine;
    return (
      <div className="rules-engine-service-control">
        <div className="image-containers">
          <img className="img-thumbnail" src="/cdap_assets/img/RulesEngine_preview_1.png" />
          <img className="img-thumbnail" src="/cdap_assets/img/RulesEngine_preview_2.png" />
        </div>
        <div className="text-container">
          <h2> {T.translate(`${PREFIX}.title`, { featureName })} </h2>
          {this.renderEnableBtn()}
          {this.renderError()}
          <p>
            {T.translate(`${PREFIX}.description`, { featureName })}
          </p>
          <div className="rules-engine-benefit">
            {T.translate(`${PREFIX}.benefits.title`, { featureName })}

            <ul>
              <li>
                <span className="fa fa-laptop" />
                <span>{T.translate(`${PREFIX}.benefits.b1`, { featureName })}</span>
              </li>
              <li>
                <IconSVG name="icon-edit" />
                <span>{T.translate(`${PREFIX}.benefits.b2`, { featureName })}</span>
              </li>
              <li>
                <IconSVG name="icon-cogs" />
                <span>{T.translate(`${PREFIX}.benefits.b3`, { featureName })}</span>
              </li>
              <li>
                <IconSVG name="icon-arrows-alt" />
                <span>{T.translate(`${PREFIX}.benefits.b4`, { featureName })}</span>
              </li>
              <li>
                <span className="fa fa-university" />
                <span>{T.translate(`${PREFIX}.benefits.b5`, { featureName })}</span>
              </li>
            </ul>
          </div>
        </div>
      </div>
    );
  }
}
