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

import * as React from 'react';
import { getCurrentNamespace } from 'services/NamespaceStore';
import { GLOBALS } from 'services/global-constants';
import { validateImportJSON } from 'services/PipelineErrorFactory';
import { Input, Label } from 'reactstrap';
import uuidV4 from 'uuid/v4';
import { objectQuery } from 'services/helpers';
import T from 'i18n-react';
import Alert from 'components/Alert';
import './EmptyList.scss';

const PREFIX = 'features.PipelineList.EmptyList';
const PIPELINE_STUDIO_STATE = 'hydrator.create';

export enum VIEW_TYPES {
  deployed = 'DEPLOYED',
  draft = 'DRAFT',
}

interface IProps {
  type: VIEW_TYPES;
}

interface IState {
  error?: string;
}

class EmptyList extends React.PureComponent<IProps, IState> {
  public state = {
    error: null,
  };

  private getPipelineParams = () => {
    return {
      namespace: getCurrentNamespace(),
      artifactType: GLOBALS.etlDataPipeline,
    };
  };

  private onError = (error) => {
    this.setState({
      error,
    });
  };

  private importHandler = (event) => {
    if (!objectQuery(event, 'target', 'files', 0)) {
      return;
    }

    const resourceCenterId = uuidV4();

    const pipelineImportLink = window.getHydratorUrl({
      stateName: PIPELINE_STUDIO_STATE,
      stateParams: {
        ...this.getPipelineParams(),
        resourceCenterId,
      },
    });

    const uploadedFile = event.target.files[0];

    if (uploadedFile.type !== 'application/json') {
      this.onError(T.translate(`${PREFIX}.nonJSONError`));
      return;
    }

    const reader = new FileReader();
    reader.readAsText(uploadedFile, 'UTF-8');

    reader.onload = (evt) => {
      if (typeof evt.target.result !== 'string') {
        return;
      }

      const fileDataString: string = evt.target.result;
      const error = validateImportJSON(fileDataString);
      if (error) {
        this.onError(error);
        return;
      }

      this.onError(null);
      window.localStorage.setItem(resourceCenterId, fileDataString);
      window.location.href = pipelineImportLink;
    };
  };

  public render() {
    const pipelineCreateLink = window.getHydratorUrl({
      stateName: PIPELINE_STUDIO_STATE,
      stateParams: this.getPipelineParams(),
    });

    return (
      <div className="pipeline-list-empty-container">
        <Alert
          message={this.state.error}
          showAlert={this.state.error && this.state.error.length > 0}
          type="error"
        />

        <div className="message-container">
          <h3>{T.translate(`${PREFIX}.Message.${this.props.type}`)}</h3>

          <hr />

          <div className="call-to-actions">
            <div className="subheading">{T.translate(`${PREFIX}.actionTitle`)}</div>

            <div className="action-row">
              <a href={pipelineCreateLink} className="action-cta">
                {T.translate(`${PREFIX}.create`)}
              </a>
              <span className="message">{T.translate(`${PREFIX}.aPipeline`)}</span>
            </div>

            <div className="action-row">
              <Label for="import-pipeline" className="action-cta">
                {T.translate(`${PREFIX}.import`)}
                <Input
                  type="file"
                  accept=".json"
                  id="import-pipeline"
                  onChange={this.importHandler}
                />
              </Label>
              <span className="message">{T.translate(`${PREFIX}.aPipeline`)}</span>
            </div>
          </div>
        </div>
      </div>
    );
  }
}

export default EmptyList;
