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
import {connect, Provider} from 'react-redux';
import DSVEditor from 'components/DSVEditor';

import MicroserviceUploadStore from 'services/WizardStores/MicroserviceUpload/MicroserviceUploadStore';
import MicroserviceUploadActions from 'services/WizardStores/MicroserviceUpload/MicroserviceUploadActions';

const mapStateToInboundQueuesProps = (state) => {
  return {
    values: state.endpoints.in
  };
};

const mapDispatchToInboundQueuesProps = (dispatch) => {
  return {
    onChange: (values) => (dispatch({
      type: MicroserviceUploadActions.setInboundQueues,
      payload: { inboundQueues: values}
    }))
  };
};

const DSVWrapper = connect(
  mapStateToInboundQueuesProps,
  mapDispatchToInboundQueuesProps
)(DSVEditor);


export default function DSVInboundQueues() {
  return (
    <Provider store={MicroserviceUploadStore}>
      <DSVWrapper />
    </Provider>
  );
}
