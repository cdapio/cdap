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
import React from 'react';
import KeyValuePairs from 'components/KeyValuePairs';
import MicroserviceUploadActions from 'services/WizardStores/MicroserviceUpload/MicroserviceUploadActions';
import MicroserviceUploadStore from 'services/WizardStores/MicroserviceUpload/MicroserviceUploadStore';
import {Provider, connect} from 'react-redux';
import T from 'i18n-react';

require('./PropertiesStep.scss');

const mapStateToKeyValProps = (state) => {
    return {
      keyValues : state.properties.keyValues
    };
  };

const mapDispatchToKeyValProps = (dispatch) => {
  return {
    onKeyValueChange: (keyValues) => {
      dispatch({
        type: MicroserviceUploadActions.setProperties,
        payload: { keyValues }
      });
    }
  };
};

const KeyValuePairsWrapper = connect(
  mapStateToKeyValProps,
  mapDispatchToKeyValProps
)(KeyValuePairs);

export default function PropertiesStep() {
  return (
    <div className="microservice-properties">
      <Provider store={MicroserviceUploadStore}>
        <KeyValuePairsWrapper
          keyPlaceholder={T.translate('features.Wizard.MicroserviceUpload.Step6.keyPlaceholder')}
        />
      </Provider>
    </div>
  );
}
