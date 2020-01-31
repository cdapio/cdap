/*
 * Copyright © 2019 Cask Data, Inc.
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
import Cookies from 'universal-cookie';

const cookie = new Cookies();

interface IExptWrapperProps {
  defaultComponent: React.ReactElement<any>;
  experimentComponent: React.ReactElement<any>;
}

const ExperimentWrapper: React.FC<IExptWrapperProps> = ({
  defaultComponent,
  experimentComponent,
}: IExptWrapperProps) => {
  const showExperiment = cookie.get('CDAP_enable_experiments');
  if (showExperiment === 'on') {
    return experimentComponent;
  } else {
    return defaultComponent;
  }
};

export default ExperimentWrapper;
