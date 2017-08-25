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
import {Form} from 'reactstrap';
import MicroserviceUploadStore from 'services/WizardStores/MicroserviceUpload/MicroserviceUploadStore';
import MicroserviceUploadActions from 'services/WizardStores/MicroserviceUpload/MicroserviceUploadActions';
import MicroserviceQueueEditor from 'components/CaskWizards/MicroserviceUpload/MicroserviceQueueEditor';
import { preventPropagation } from 'services/helpers';

const mapStateToOutboundQueuesProps = (state) => {
  return {
    values: state.outboundQueues.queues
  };
};

const mapDispatchToOutboundQueuesProps = (dispatch) => {
  return {
    onChange: (values) => (dispatch({
      type: MicroserviceUploadActions.setOutboundQueues,
      payload: { outboundQueues: values }
    }))
  };
};

const MicroserviceQueueWrapper = connect(
  mapStateToOutboundQueuesProps,
  mapDispatchToOutboundQueuesProps
)(MicroserviceQueueEditor);

export default function OutboundQueueStep() {
  return (
    <Provider store={MicroserviceUploadStore}>
      <Form
        className="form-horizontal"
        onSubmit={(e) => {
          preventPropagation(e);
          return false;
        }}
      >
        <MicroserviceQueueWrapper />
      </Form>
    </Provider>
  );
}
