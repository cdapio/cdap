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

import React, { useState } from 'react';
import ToggleSwitchWidget from 'components/AbstractWidget/ToggleSwitchWidget';
import Cookies from 'universal-cookie';

const cookie = new Cookies();

const ExperimentToggle = () => {
  const initSetting = cookie.get('CDAP_enable_experiments') || 'Off';
  const [showExperiments, toggleExperiments] = useState(initSetting);
  const onChangeHandler = (exptSetting: string) => {
    toggleExperiments(exptSetting);
    cookie.set('CDAP_enable_experiments', exptSetting, { path: '/' });
  };

  return (
    <div className="container">
      <h1>Dev Experiments</h1>
      <p>To see dev experiments, set to On. Cookies must be enabled.</p>
      <ToggleSwitchWidget onChange={onChangeHandler} value={showExperiments} />
    </div>
  );
};

export default ExperimentToggle;
