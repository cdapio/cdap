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

import ProvisionerInfoStore, {ACTIONS} from 'components/Cloud/Store';
import {MyCloudApi} from 'api/cloud';

const fetchProvisionerSpec = (provisionerName) => {
  ProvisionerInfoStore.dispatch({
    type: ACTIONS.SET_LOADING,
    payload: {
      loading: true
    }
  });

  MyCloudApi
    .getProvisionerDetailSpec({
      provisioner: provisionerName
    })
    .subscribe(
      (provisionerSpec) => {
        ProvisionerInfoStore.dispatch({
          type: ACTIONS.SET_JSON_SPEC,
          payload: {
            provisionerName,
            provisionerSpec
          }
        });
      },
      (error) => {
        ProvisionerInfoStore.dispatch({
          type: ACTIONS.SET_ERROR,
          payload: {
            error
          }
        });
      }
    );
};

export { fetchProvisionerSpec };
