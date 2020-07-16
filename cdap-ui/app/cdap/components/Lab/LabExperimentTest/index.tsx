/*
 * Copyright © 2020 Cask Data, Inc.
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

import ExperimentalFeature from 'components/Lab/ExperimentalFeature';
import ToggleExperiment from 'components/Lab/ToggleExperiment';

const DEFAULT_EXPERIMENT = 'cdap-common-experiment';

const LabExperimentTest: React.FC = () => {
  return (
    <div>
      <ExperimentalFeature experimentId={DEFAULT_EXPERIMENT}>
        <p data-cy="experimental-feature-selector">This is an experimental component.</p>
      </ExperimentalFeature>
      <ToggleExperiment
        experimentId={DEFAULT_EXPERIMENT}
        defaultComponent={
          <p data-cy="default-feature-toggle-selector">This is default component for the toggle.</p>
        }
        experimentalComponent={
          <p data-cy="experimental-feature-toggle-selector">
            This is experimental component for the toggle.
          </p>
        }
      />
    </div>
  );
};

export default LabExperimentTest;
