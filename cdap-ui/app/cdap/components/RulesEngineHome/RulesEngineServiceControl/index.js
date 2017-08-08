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

import React, {Component, PropTypes} from 'react';
import MyRuleEngineApi from 'api/rulesengine';
import enableDataPreparationService from 'components/DataPrep/DataPrepServiceControl/ServiceEnablerUtilities';
import LoadingSVG from 'components/LoadingSVG';
import T from 'i18n-react';

require('./RulesEngineServiceControl.scss');
const PREFIX = 'features.RulesEngine.RulesEngineServiceControl';

export default class RulesEngineServiceControl extends Component {

  static propTypes = {
    onServiceStart: PropTypes.func
  };

  state = {
    loading: false
  };

  enableRulesEngine = () => {
    this.setState({
      loading: true
    });
    enableDataPreparationService({
      shouldStopService: false,
      artifactName: 'yare-service',
      api: MyRuleEngineApi,
      i18nPrefix: ''
    })
      .subscribe(
        this.props.onServiceStart
      );
  }

  render() {
    return (
      <div className="rules-engine-service-control">
        <div className="image-containers">
          <img src="/cdap_assets/img/RulesEnginePreview1.png" />
          <img src="/cdap_assets/img/RulesEnginePreview2.png" />
        </div>
        <div className="text-container">
          <h2> {T.translate(`${PREFIX}.title`)} </h2>
          <p>
            {T.translate(`${PREFIX}.description`)}
          </p>
          <div className="rules-engine-benefit">
            {T.translate(`${PREFIX}.benefits.title`)}

            <ul>
              <li>{T.translate(`${PREFIX}.benefits.b1`)}</li>
              <li>{T.translate(`${PREFIX}.benefits.b2`)}</li>
              <li>{T.translate(`${PREFIX}.benefits.b3`)}</li>
              <li>{T.translate(`${PREFIX}.benefits.b4`)}</li>
            </ul>
          </div>
          <button
            className="btn btn-primary"
            onClick={this.enableRulesEngine}
            disabled={this.state.loading}
          >
            {
              this.state.loading ?
                <LoadingSVG height="16px"/>
              :
                null
            } <span className="btn-label">{T.translate(`${PREFIX}.enableBtnLabel`)}</span>
          </button>
        </div>
      </div>
    );
  }
}
