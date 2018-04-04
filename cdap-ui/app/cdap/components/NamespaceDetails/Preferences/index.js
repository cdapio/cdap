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
import {connect} from 'react-redux';
import PropTypes from 'prop-types';
import T from 'i18n-react';
import SortableTable from 'components/SortableTable';
import SetPreferenceModal, {PREFERENCES_LEVEL} from 'components/FastAction/SetPreferenceAction/SetPreferenceModal';
import {getNamespacePrefs} from 'components/NamespaceDetails/store/ActionCreator';
import ee from 'event-emitter';
import globalEvents from 'services/global-events';
import classnames from 'classnames';
import ViewAllLabel from 'components/ViewAllLabel';
require('./Preferences.scss');

const PREFIX = 'features.NamespaceDetails.preferences';

const PREFERENCES_TABLE_HEADERS = [
  {
    property: 'key',
    label: T.translate('commons.keyValPairs.keyLabel'),
    defaultSortBy: true
  },
  {
    property: 'scope',
    label: T.translate('commons.scope')
  },
  {
    property: 'value',
    label: T.translate('commons.keyValPairs.valueLabel')
  }
];

class NamespaceDetailsPreferences extends Component {
  state = {
    modalOpen: false,
    prefs: this.getPrefsForDisplay(this.props.namespacePrefs, this.props.systemPrefs),
    newNamespacePrefsKeys: [],
    viewAll: false
  };

  static propTypes = {
    namespacePrefs: PropTypes.object,
    systemPrefs: PropTypes.object,
  };

  eventEmitter = ee(ee);

  componentDidMount() {
    this.eventEmitter.on(globalEvents.NSPREFERENCESSAVED, getNamespacePrefs);
  }

  componentWillReceiveProps(nextProps) {
    let newNamespacePrefsKeys = [];
    let currentPrefsKeys = this.state.prefs.map(pref => pref.key);

    Object.keys(nextProps.namespacePrefs).forEach(nextNSPrefKey => {
      let nextNSPrefKeyIndex = currentPrefsKeys.indexOf(nextNSPrefKey);
      if (nextNSPrefKeyIndex === -1 || (nextNSPrefKeyIndex !== -1 && this.state.prefs[nextNSPrefKeyIndex].value !== nextProps.namespacePrefs[nextNSPrefKey])) {
        newNamespacePrefsKeys.push(nextNSPrefKey);
      }
    });

    let prefs = this.getPrefsForDisplay(nextProps.namespacePrefs, nextProps.systemPrefs);
    let viewAll = this.state.viewAll;

    if (!viewAll && newNamespacePrefsKeys.length && prefs.length >= 5) {
      for (let i = 5; i < prefs.length; i++) {
        if (newNamespacePrefsKeys.indexOf(prefs[i].key) !== -1) {
          viewAll = true;
          break;
        }
      }
    }

    this.setState({
      prefs,
      newNamespacePrefsKeys,
      viewAll
    }, () => {
      setTimeout(() => {
        this.setState({
          newNamespacePrefsKeys: []
        });
      }, 3000);
    });
  }

  componentDidUpdate() {
    let highlightedElems = document.getElementsByClassName('highlighted');
    if (highlightedElems.length) {
      highlightedElems[0].scrollIntoView();
    }
  }

  componentWillUnmount() {
    this.eventEmitter.off(globalEvents.NSPREFERENCESSAVED, getNamespacePrefs);
  }

  getPrefsForDisplay(namespacePrefs, systemPrefs) {
    let prefs = {...systemPrefs, ...namespacePrefs};
    let sortedPrefObjectKeys = [...Object.keys(prefs)].sort();
    return sortedPrefObjectKeys.map(prefKey => {
      return {
        key: prefKey,
        scope: prefKey in namespacePrefs ? T.translate('features.NamespaceDetails.namespace') : T.translate('commons.cdap'),
        value: prefs[prefKey],
      };
    });
  }

  toggleModal = () => {
    this.setState({
      modalOpen: !this.state.modalOpen
    });
  };

  toggleViewAll = () => {
    this.setState({
      viewAll: !this.state.viewAll
    });
  }

  renderPreferencesLabel() {
    let label;
    if (!this.state.prefs.length) {
      label = <strong>{T.translate(`${PREFIX}.label`)}</strong>;
    } else {
      label = <strong>{T.translate(`${PREFIX}.labelWithCount`, {count: this.state.prefs.length})}</strong>;
    }

    return (
      <div className="namespace-details-section-label">
        {label}
        <span
          className="edit-label"
          onClick={this.toggleModal}
        >
          {T.translate('features.NamespaceDetails.edit')}
        </span>
      </div>
    );
  }

  renderPreferencesTable() {
    if (!this.state.prefs.length) {
      return (
        <div className="text-xs-center">
          {T.translate(`${PREFIX}.noPreferences`)}
        </div>
      );
    }

    let prefs = [...this.state.prefs];

    if (!this.state.viewAll && prefs.length > 5) {
      prefs = prefs.slice(0, 5);
    }

    // have to do this in a render function since this.state.newNamespacePrefsKeys is emptied after 3 secs
    prefs = prefs.map(pref => {
      return {
        ...pref,
        highlighted: this.state.newNamespacePrefsKeys.indexOf(pref.key) !== -1
      };
    });

    return (
      <SortableTable
        entities={prefs}
        tableHeaders={PREFERENCES_TABLE_HEADERS}
        renderTableBody={this.renderPreferencesTableBody}
        className="preferences-table"
        sortOnInitialLoad={true}
      />
    );
  }

  renderPreferencesTableBody = (prefs) => {
    return (
      <tbody>
        {
          prefs
            .map((pref, index) => {
              return (
                <tr
                  className={classnames({highlighted: pref.highlighted})}
                  key={index}
                >
                  <td title={pref.key}>
                    {pref.key}
                  </td>
                  <td>{pref.scope}</td>
                  <td title={pref.value}>
                    {pref.value}
                  </td>
                </tr>
              );
            })
        }
      </tbody>
    );
  };

  render() {
    return (
      <div className="namespace-details-preferences">
        {this.renderPreferencesLabel()}
        {this.renderPreferencesTable()}
        <ViewAllLabel
          arrayToLimit={this.state.prefs}
          limit={5}
          viewAllState={this.state.viewAll}
          toggleViewAll={this.toggleViewAll}
        />
        {
          this.state.modalOpen ?
            <SetPreferenceModal
              isOpen={this.state.modalOpen}
              toggleModal={this.toggleModal}
              setAtLevel={PREFERENCES_LEVEL.NAMESPACE}
            />
          :
            null
        }
      </div>
    );
  }
}

const mapStateToProps = (state) => {
  return {
    namespacePrefs: state.namespacePrefs,
    systemPrefs: state.systemPrefs
  };
};

const ConnectedNamespaceDetailsPreferences = connect(mapStateToProps)(NamespaceDetailsPreferences);
export default ConnectedNamespaceDetailsPreferences;
