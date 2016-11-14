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

import React, {PropTypes} from 'react';
import ArtifactUploadWizard from 'components/CaskWizards/ArtifactUpload';
import ArtifactUploadStore from 'services/WizardStores/ArtifactUpload/ArtifactUploadStore';
import ArtifactUploadActions from 'services/WizardStores/ArtifactUpload/ArtifactUploadActions';
import {MyMarketApi} from 'api/market';
import find from 'lodash/find';

export default function MarketArtifactUploadWizard({input, onClose, isOpen}) {
  const args = input.action.arguments;
  let config = find(args, {name: 'config'});

  let params = {
    entityName: input.package.name,
    entityVersion: input.package.version,
    filename: config.value
  };

  MyMarketApi.getSampleData(params)
    .subscribe((res) => {
      const plugin = res.plugins[0];

      ArtifactUploadStore.dispatch({
        type: ArtifactUploadActions.setName,
        payload: { name: plugin.name }
      });

      ArtifactUploadStore.dispatch({
        type: ArtifactUploadActions.setDescription,
        payload: { description: plugin.description }
      });

      ArtifactUploadStore.dispatch({
        type: ArtifactUploadActions.setClassname,
        payload: { classname: plugin.className }
      });

    });
  return (
    <ArtifactUploadWizard
      isOpen={isOpen}
      input={input}
      onClose={onClose}
      isMarket={true}
    />
  );
}

MarketArtifactUploadWizard.propTypes = {
  isOpen: PropTypes.bool,
  input: PropTypes.any,
  onClose: PropTypes.func
};
