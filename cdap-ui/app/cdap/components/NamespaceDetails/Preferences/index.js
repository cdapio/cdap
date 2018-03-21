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
import isEmpty from 'lodash/isEmpty';
import SortableTable from 'components/SortableTable';
import SetPreferenceModal from 'components/FastAction/SetPreferenceAction/SetPreferenceModal';
import {getNamespacePrefs} from 'components/NamespaceDetails/store/ActionCreator';
require('./Preferences.scss');

const PREFIX = 'features.NamespaceDetails.preferences';

const PREFERENCES_TABLE_HEADERS = [
  {
    property: 'key',
    label: T.translate('commons.keyValPairs.keyLabel')
  },
  {
    property: 'scope',
    label: T.translate(`${PREFIX}.scope`)
  },
  {
    property: 'value',
    label: T.translate('commons.keyValPairs.valueLabel')
  }
];

const renderPreferencesTableBody = (prefs) => {
  return (
    <tbody>
      {
        prefs
          .map((pref, index) => {
            return (
              <tr key={index}>
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

const convertPrefsArray = (prefs, scope) => {
  return Object.keys(prefs).map(prefKey => {
    return {
      key: prefKey,
      scope,
      value: prefs[prefKey]
    };
  });
};

const mapStateToProps = (state) => {
  return {
    namespacePrefs: state.namespacePrefs,
    systemPrefs: state.systemPrefs
  };
};

const PreferencesTable = ({namespacePrefs, systemPrefs}) => {
  if (isEmpty(namespacePrefs) && isEmpty(systemPrefs)) {
    return (
      <div className="text-xs-center">
        {T.translate(`${PREFIX}.noPreferences`)}
      </div>
    );
  }

  namespacePrefs = convertPrefsArray(namespacePrefs, T.translate('features.NamespaceDetails.namespace'));
  systemPrefs = convertPrefsArray(systemPrefs, T.translate('commons.cdap'));
  let prefs = namespacePrefs.concat(systemPrefs);

  return (
    <SortableTable
      entities={prefs}
      tableHeaders={PREFERENCES_TABLE_HEADERS}
      renderTableBody={renderPreferencesTableBody}
      className="preferences-table"
      sortOnInitialLoad={false}
    />
  );
};

PreferencesTable.propTypes = {
  namespacePrefs: PropTypes.object,
  systemPrefs: PropTypes.object
};

const PreferencesLabel = ({namespacePrefs, systemPrefs, toggleModal}) => {
  let label;
  if (isEmpty(namespacePrefs) && isEmpty(systemPrefs)) {
    label = <strong>{T.translate(`${PREFIX}.label`)}</strong>;
  } else {
    let prefsCount = Object.keys(namespacePrefs).length + Object.keys(systemPrefs).length;
    label = <strong>{T.translate(`${PREFIX}.labelWithCount`, {count: prefsCount})}</strong>;
  }

  return (
    <div className="namespace-details-section-label">
      {label}
      <span
        className="edit-label"
        onClick={toggleModal}
      >
        {T.translate('features.NamespaceDetails.edit')}
      </span>
    </div>
  );
};

PreferencesLabel.propTypes = {
  namespacePrefs: PropTypes.object,
  systemPrefs: PropTypes.object,
  toggleModal: PropTypes.func
};

const ConnectedPreferencesTable = connect(mapStateToProps)(PreferencesTable);
const ConnectedPreferencesLabel = connect(mapStateToProps)(PreferencesLabel);

export default class NamespaceDetailsPreferences extends Component {
  state = {
    modalOpen: false
  };

  toggleModal = () => {
    this.setState({
      modalOpen: !this.state.modalOpen
    });
  };

  render() {
    return (
      <div className="namespace-details-preferences">
        <ConnectedPreferencesLabel
          toggleModal={this.toggleModal}
        />
        <ConnectedPreferencesTable />
        {
          this.state.modalOpen ?
            <SetPreferenceModal
              isOpen={this.state.modalOpen}
              toggleModal={this.toggleModal}
              onSuccess={getNamespacePrefs}
            />
          :
            null
        }
      </div>
    );
  }
}
