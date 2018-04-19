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

import React, {Component} from 'react';
import PropTypes from 'prop-types';
import {Link, Route, Switch} from 'react-router-dom';
import {humanReadableDuration} from 'services/helpers';
import VersionStore from 'services/VersionStore';
import VersionActions from 'services/VersionStore/VersionActions';
import MyCDAPVersionApi from 'api/version';
import isNil from 'lodash/isNil';
import classnames from 'classnames';
import T from 'i18n-react';
require('./AdminTabSwitch.scss');

const PREFIX = 'features.Administration';

export default class AdminTabSwitch extends Component {
  state = {
    version: null
  };

  static propTypes = {
    uptime: PropTypes.number
  };

  componentDidMount() {
    if (!VersionStore.getState().version) {
      this.getCDAPVersion();
    } else {
      this.setState({ version : VersionStore.getState().version });
    }
  }

  getCDAPVersion() {
    MyCDAPVersionApi
      .get()
      .subscribe((res) => {
        this.setState({ version : res.version });
        VersionStore.dispatch({
          type: VersionActions.updateVersion,
          payload: {
            version: res.version
          }
        });
      });
  }

  renderTabTitle(isManagement = true) {
    return (
      <span className="tab-title">
        <h5 className={classnames({"active": isManagement})}>
          <Link to='/administration'>
            {T.translate(`${PREFIX}.Tabs.management`)}
          </Link>
        </h5>
        <span className="divider"> | </span>
        <h5 className={classnames({"active": !isManagement})}>
          <Link to='/administration/configuration'>
            {T.translate(`${PREFIX}.Tabs.config`)}
          </Link>
        </h5>
      </span>
    );
  }

  renderUptimeVersion() {
    return (
      <span className="uptime-version-container">
        <span>
          {
            this.props.uptime ?
              T.translate(`${PREFIX}.uptimeLabel`, {
                time: humanReadableDuration(Math.ceil(this.props.uptime / 1000))
              })
            :
              null
          }
        </span>
        {
          isNil(this.state.version) ?
            null
          :
            (
              <i className="cdap-version">
                {T.translate(`${PREFIX}.Top.version-label`)} - {this.state.version}
              </i>
            )
        }
      </span>
    );
  }

  render() {
    return (
      <Switch>
        <Route exact path="/administration" render={() => {
          return (
            <div className="tab-title-and-version">
              {this.renderTabTitle()}
              {this.renderUptimeVersion()}
            </div>
          );
        }} />
        <Route exact path="/administration/configuration" render={() => {
          return (
            <div className="tab-title-and-version">
              {this.renderTabTitle(false)}
            </div>
          );
        }} />
      </Switch>
    );
  }
}
