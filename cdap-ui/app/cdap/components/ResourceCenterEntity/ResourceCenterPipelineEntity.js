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

import PropTypes from 'prop-types';
import React from 'react';
import PlusButtonStore from 'services/PlusButtonStore';
import NamespaceStore from 'services/NamespaceStore';
import {validateImportJSON} from 'services/PipelineErrorFactory';
import {objectQuery} from 'services/helpers';
import IconSVG from 'components/IconSVG';
import { Input, Label } from 'reactstrap';
import T from 'i18n-react';
import uuidV4 from 'uuid/v4';

require('./ResourceCenterPipelineEntity.scss');

const PREFIX = 'features.Resource-Center.HydratorPipeline';

export default function ResourceCenterPipelineEntity({onError}) {
  const title = T.translate(`${PREFIX}.label`);
  const description = T.translate(`${PREFIX}.description`);
  const primaryActionLabel = T.translate(`${PREFIX}.actionbtn0`);
  const secondaryActionLabel = T.translate(`${PREFIX}.actionbtn1`);
  const iconClassName = 'icon-pipelines';

  const resourceCenterId = uuidV4();
  const hydratorLinkStateName = 'hydrator.create';
  const hydratorLinkStateParams = {
    namespace: NamespaceStore.getState().selectedNamespace,
    artifactType: 'cdap-data-pipeline'
  };

  const hydratorCreateLink = window.getHydratorUrl({
    stateName: hydratorLinkStateName,
    stateParams: hydratorLinkStateParams
  });
  const hydratorImportLink = window.getHydratorUrl({
    stateName: hydratorLinkStateName,
    stateParams: {
      ...hydratorLinkStateParams,
      resourceCenterId
    }
  });

  const createBtnHandler = () => {
    PlusButtonStore.dispatch({
      type: 'TOGGLE_PLUSBUTTON_MODAL',
      payload: {
        modalState: false
      }
    });
  };

  const importBtnHandler = (event) => {
    if (!objectQuery(event, 'target', 'files', 0)) {
      return;
    }

    let uploadedFile = event.target.files[0];

    if (uploadedFile.type !== 'application/json') {
      onError(T.translate(`${PREFIX}.errorLabel`), T.translate(`${PREFIX}.nonJSONError`));
      return;
    }

    let reader = new FileReader();
    reader.readAsText(uploadedFile, 'UTF-8');

    reader.onload =  (evt) => {
      let fileDataString = evt.target.result;
      let error = validateImportJSON(fileDataString);
      if (error) {
        onError(T.translate(`${PREFIX}.errorLabel`), error);
        return;
      }

      onError(null, null);
      window.localStorage.setItem(resourceCenterId, fileDataString);
      window.location.href = hydratorImportLink;
    };
  };

  return (
    <div className="resourcecenter-entity-card resourcecenter-entity-card-pipeline">
      <div className="image-content-container">
        <div className="image-container">
          <div className="entity-image">
            <IconSVG name={iconClassName} />
          </div>
        </div>
        <div className="content-container">
          <div className="content-text">
            <h4>{title}</h4>
            <p>{description}</p>
          </div>
        </div>
      </div>
      <div className="buttons-container">
        <a href={hydratorCreateLink}>
          <button
            id={(primaryActionLabel + "-" + title).toLowerCase()}
            className="btn btn-primary"
            onClick={createBtnHandler}
          >
            {primaryActionLabel}
          </button>
        </a>
        <Label
          for="resource-center-import-pipeline"
          id={(secondaryActionLabel + "-" + title).toLowerCase()}
          className="btn btn-secondary"
        >
          {secondaryActionLabel}
          <Input
            type="file"
            accept='.json'
            id="resource-center-import-pipeline"
            onChange={importBtnHandler}
          />
        </Label>
      </div>
    </div>
  );
}
ResourceCenterPipelineEntity.propTypes = {
  onError: PropTypes.func
};
