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
import IconSVG from 'components/IconSVG';

require('./PipelineModelessOld.scss');

export default function PipelineModelessOld({ title, onClose, children }) {
  return (
    <div className="pipeline-modeless-old-container">
      <div className="pipeline-modeless-header">
        <div className="pipeline-modeless-title">{title}</div>
        <div className="btn-group">
          <a className="btn" onClick={onClose}>
            <IconSVG name="icon-close" />
          </a>
        </div>
      </div>
      <div className="pipeline-modeless-content">{children}</div>
    </div>
  );
}

PipelineModelessOld.propTypes = {
  title: PropTypes.node,
  onClose: PropTypes.func,
  children: PropTypes.node,
};
