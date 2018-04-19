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
import {convertMapToKeyValuePairs, convertKeyValuePairsToMap} from 'services/helpers';
import {MyPreferenceApi} from 'api/preference';
import ViewAllLabel from 'components/ViewAllLabel';
import T from 'i18n-react';
import isEqual from 'lodash/isEqual';
import SortableStickyGrid from 'components/SortableStickyGrid';
import {PREFERENCES_LEVEL} from 'components/FastAction/SetPreferenceAction/SetPreferenceModal';

const PREFIX = 'features.Administration.Accordions.SystemPrefs';

const GRID_HEADERS = [
  {
    property: 'key',
    label: T.translate('commons.keyValPairs.keyLabel')
  },
  {
    property: 'value',
    label: T.translate('commons.keyValPairs.valueLabel')
  }
];

const NUM_PREFS_TO_SHOW = 5;

export default class SystemPrefsAccordion extends Component {
  state = {
    prefsModalOpen: false,
    prefsForDisplay: convertMapToKeyValuePairs(this.props.prefs, false),
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
        prefsForDisplay: convertMapToKeyValuePairs(nextProps.prefs, false)
      });
    }
  }

  fetchPrefs = () => {
    MyPreferenceApi
      .getSystemPreferences()
      .subscribe(
        (prefs) => {
          let currentPrefs = convertKeyValuePairsToMap(this.state.prefsForDisplay);
          let hasNewPrefs = false;

          let newPrefsForDisplay = Object.entries(prefs).map(([key, value]) => {
            let prefIsHighlighted = false;

            if (!(key in currentPrefs) || (key in currentPrefs && currentPrefs[key] !== value)) {
              hasNewPrefs = true;
              prefIsHighlighted = true;
            }
            return {
              key,
              value,
              highlighted: prefIsHighlighted
            };
          });

          this.setState({
            prefsForDisplay: newPrefsForDisplay,
            viewAll: hasNewPrefs || this.state.viewAll
          }, () => {
            if (hasNewPrefs) {
              setTimeout(() => {
                newPrefsForDisplay = newPrefsForDisplay.map(pref => {
                  return {
                    key: pref.key,
                    value: pref.value,
                    highlighted: false
                  };
                });
                this.setState({
                  prefsForDisplay: newPrefsForDisplay
                });
              }, 4000);
            }
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

    if (!this.state.viewAll && prefs.length > NUM_PREFS_TO_SHOW) {
      prefs = prefs.slice(0, NUM_PREFS_TO_SHOW);
    }

    return (
      <SortableStickyGrid
        entities={prefs}
        gridHeaders={GRID_HEADERS}
      />
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
        <ViewAllLabel
          arrayToLimit={this.state.prefsForDisplay}
          limit={NUM_PREFS_TO_SHOW}
          viewAllState={this.state.viewAll}
          toggleViewAll={this.toggleViewAll}
        />
        {this.renderGrid()}
        <ViewAllLabel
          arrayToLimit={this.state.prefsForDisplay}
          limit={NUM_PREFS_TO_SHOW}
          viewAllState={this.state.viewAll}
          toggleViewAll={this.toggleViewAll}
        />
        {
          this.state.prefsModalOpen ?
            <SetPreferenceModal
              isOpen={this.state.prefsModalOpen}
              toggleModal={this.togglePrefsModal}
              onSuccess={this.fetchPrefs}
              setAtLevel={PREFERENCES_LEVEL.SYSTEM}
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
