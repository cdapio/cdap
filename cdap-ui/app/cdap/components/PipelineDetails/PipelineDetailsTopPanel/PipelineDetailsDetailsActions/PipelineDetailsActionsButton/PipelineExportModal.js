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

import PropTypes from 'prop-types';
import React from 'react';
import { ModalBody, ModalFooter } from 'reactstrap';
import HydratorModal from 'components/HydratorModal';
import IconSVG from 'components/IconSVG';

export default function PipelineExportModal({isOpen, onClose, pipelineConfig}) {
  const exportPipeline = () => {
    let blob = new Blob([JSON.stringify(pipelineConfig, null, 4)], { type: 'application/json'});
    let url = URL.createObjectURL(blob);
    let exportFileName = (pipelineConfig.name? pipelineConfig.name: 'noname') + '-' + pipelineConfig.artifact.name;
    let aElement = document.getElementById('pipeline-export-config-link');
    aElement.href = url;
    aElement.download = exportFileName + '.json';
    aElement.click();
    if (typeof onClose === 'function') {
      onClose();
    }
  };

  return (
    <HydratorModal
      isOpen={isOpen}
      toggle={onClose}
      size="lg"
      backdrop="static"
      modalClassName="pipeline-export-modal hydrator-modal"
    >
      <div className="modal-header">
        <h3 className="modal-title float-xs-left">
          <span>Export Pipeline Configuration</span>
        </h3>
        <div className="btn-group float-xs-right">
          <a
            className="btn"
            onClick={onClose}
          >
            <IconSVG name = "icon-close" />
          </a>
        </div>
      </div>

      <ModalBody>
        <fieldset disabled className="view-plugin-json">
          <div className="widget-json-editor">
            <div className="textarea-container">
              <textarea
                className="form-control"
                value={JSON.stringify(pipelineConfig, null, 2)}
              />
            </div>
          </div>
        </fieldset>
      </ModalBody>
      <ModalFooter>
        <div
          className="btn btn-grey-cancel close-button"
          onClick={onClose}
        >
          Close
        </div>
        <div
          className="btn btn-blue"
          onClick={exportPipeline}
        >
          Export
          <a id="pipeline-export-config-link" />
        </div>
      </ModalFooter>
    </HydratorModal>
  );
}

PipelineExportModal.propTypes = {
  isOpen: PropTypes.bool,
  onClose: PropTypes.func,
  pipelineConfig: PropTypes.object
};
