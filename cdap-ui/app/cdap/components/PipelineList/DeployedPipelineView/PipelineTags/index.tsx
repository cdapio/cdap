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
import Tags from 'components/Tags';
import { IPipeline } from 'components/PipelineList/DeployedPipelineView/types';

import './PipelineTags.scss';

interface IProps {
  pipeline: IPipeline;
}

const PipelineTags: React.SFC<IProps> = ({ pipeline }) => {
  return (
    <div className="tags">
      <Tags
        entity={{
          id: pipeline.name,
          type: 'application',
        }}
        showCountLabel={false}
        viewOnly={true}
        displayAll={true}
        isNativeLink={true}
        preventDefault={true}
      />
    </div>
  );
};

export default PipelineTags;
