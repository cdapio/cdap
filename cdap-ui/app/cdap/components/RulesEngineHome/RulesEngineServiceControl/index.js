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

require('./RulesEngineServiceControl.scss');

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
      artifactName: 'rules-engine-service',
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
          <h2> Welcome Rules Engine Management </h2>
          <p>
            Rules Engine is a sophisticated if-then-else statement interpreter that runs natively on big data system like Spark and Hadoop.
            It provides an alternative computational model for transforming your data while empowering the business
            users to specify and manage the transformations and policy enforcements.
          </p>
          <div className="rules-engine-benefit">
            Some of the benefits of Rules Engine are,

            <ul>
              <li>Non-Programmers who want to analyze big data</li>
              <li>One-time infrastructure setup</li>
              <li>Separation of Logic and Data</li>
              <li>Speed and Scalability</li>
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
            } <span className="btn-label">Enable Rules Engine</span>
          </button>
        </div>
      </div>
    );
  }
}
