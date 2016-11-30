/*
 * Copyright © 2016 Cask Data, Inc.
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
import React from 'react';
import KeyValuePairs from 'components/KeyValuePairs';
import AddNamespaceActions from 'services/WizardStores/AddNamespace/AddNamespaceActions';
import AddNamespaceStore from 'services/WizardStores/AddNamespace/AddNamespaceStore';
import {Provider, connect} from 'react-redux';
require('./PreferencesStep.less');

export default function PreferencesStep() {

  const mapStateToKeyValProps = (state) => {
    return {
      keyValues : state.preferences.keyValues
    };
  };

  const mapDispatchToKeyValProps = (dispatch) => {
    return {
      onKeyValueChange: (keyValues) => {
        dispatch({
          type: AddNamespaceActions.setPreferences,
          payload: { keyValues }
        });
      }
    };
  };

  const KeyValuePairsWrapper = connect(
    mapStateToKeyValProps,
    mapDispatchToKeyValProps
  )(KeyValuePairs);

  return (
    <div className="namespace-preferences">
      <Provider store={AddNamespaceStore}>
        <KeyValuePairsWrapper />
      </Provider>
    </div>
  );
}
