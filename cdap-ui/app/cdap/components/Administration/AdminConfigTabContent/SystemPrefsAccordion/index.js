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
import IconSVG from 'components/IconSVG';
import SetPreferenceModal from 'components/FastAction/SetPreferenceAction/SetPreferenceModal';
import classnames from 'classnames';
import {convertMapToKeyValuePairs} from 'services/helpers';
import {MyPreferenceApi} from 'api/preference';
import ViewAllLabel from 'components/ViewAllLabel';
import T from 'i18n-react';
import isEqual from 'lodash/isEqual';

const PREFIX = 'features.Administration.Accordions.SystemPrefs';

export default class SystemPrefsAccordion extends Component {
  state = {
    prefsModalOpen: false,
    prefsForDisplay: convertMapToKeyValuePairs(this.props.prefs),
    viewAll: false
  };

  static propTypes = {
    prefs: PropTypes.object,
    loading: PropTypes.bool,
    expanded: PropTypes.bool,
    onExpand: PropTypes.func
  };

  componentWillReceiveProps(nextProps) {
    if (!isEqual(this.props.prefs, nextProps.prefs)) {
      this.setState({
        prefsForDisplay: convertMapToKeyValuePairs(nextProps.prefs)
      });
    }
  }

  fetchPrefs = () => {
    MyPreferenceApi
      .getSystemPreferences()
      .subscribe(
        (prefs) => {
          this.setState({
            prefsForDisplay: convertMapToKeyValuePairs(prefs),
          });
        },
        (err) => console.log(err)
      );
  }

  togglePrefsModal = () => {
    this.setState({
      prefsModalOpen: !this.state.prefsModalOpen
    });
  }

  toggleViewAll = () => {
    this.setState({
      viewAll: !this.state.viewAll
    });
  }

  renderLabel() {
    return (
      <div
        className="admin-config-container-toggle"
        onClick={this.props.onExpand}
      >
        <span className="admin-config-container-label">
          <IconSVG name={this.props.expanded ? "icon-caret-down" : "icon-caret-right"} />
          {
            this.props.loading ?
              (
                <h5>
                  {T.translate(`${PREFIX}.label`)}
                  <IconSVG name="icon-spinner" className="fa-spin" />
                </h5>
              )
            :
              <h5>{T.translate(`${PREFIX}.labelWithCount`, {count: this.state.prefsForDisplay.length})}</h5>
          }
        </span>
        <span className="admin-config-container-description">
          {T.translate(`${PREFIX}.description`)}
        </span>
      </div>
    );
  }

  renderGrid() {
    if (!this.state.prefsForDisplay.length) {
      return (
        <div className="grid-wrapper text-xs-center">
          {T.translate(`${PREFIX}.noPrefs`)}
        </div>
      );
    }

    let prefs = [...this.state.prefsForDisplay];

    if (!this.state.viewAll && prefs.length > 5) {
      prefs = prefs.slice(0, 5);
    }

    return (
      <div className="grid-wrapper">
        <div className="grid grid-container">
          <div className="grid-header">
            <div className="grid-row">
              <strong>{T.translate('commons.keyValPairs.keyLabel')}</strong>
              <strong>{T.translate('commons.keyValPairs.valueLabel')}</strong>
            </div>
          </div>
          <div className="grid-body">
            {
              prefs.map((pref, i) => {
                return (
                  <div className="grid-row" key={i}>
                    <div>{pref.key}</div>
                    <div>{pref.value}</div>
                  </div>
                );
              })
            }
          </div>
        </div>
      </div>
    );
  }

  renderContent() {
    if (!this.props.expanded) {
      return null;
    }

    return (
      <div className="admin-config-container-content system-prefs-container-content">
        <button
          className="btn btn-secondary"
          onClick={this.togglePrefsModal}
        >
          {T.translate(`${PREFIX}.create`)}
        </button>
        {this.renderGrid()}
        <ViewAllLabel
          arrayToLimit={this.state.prefsForDisplay}
          limit={5}
          viewAllState={this.state.viewAll}
          toggleViewAll={this.toggleViewAll}
        />
        {
          this.state.prefsModalOpen ?
            <SetPreferenceModal
              isOpen={this.state.prefsModalOpen}
              toggleModal={this.togglePrefsModal}
              onSuccess={this.fetchPrefs}
            />
          :
            null
        }
      </div>
    );
  }

  render() {
    return (
      <div className={classnames(
        "admin-config-container system-prefs-container",
        {"expanded": this.props.expanded}
      )}>
        {this.renderLabel()}
        {this.renderContent()}
      </div>
    );
  }
}
