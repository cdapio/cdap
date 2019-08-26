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

import React, { useContext } from 'react';
import classnames from 'classnames';
import IconSVG from 'components/IconSVG';
import { FllContext, IContextState } from 'components/FieldLevelLineage/v2/Context/FllContext';

const NavigationView: React.FC = () => {
  const { activeOpsIndex, operations, prevOperation, nextOperation } = useContext<IContextState>(
    FllContext
  );
  const limit = operations.length;
  const prevDisabled = activeOpsIndex === 0;
  const nextDisabled = activeOpsIndex === limit - 1;

  return (
    <div className="navigation">
      <span
        className={classnames('nav-icon', { disabled: prevDisabled })}
        onClick={!prevDisabled ? prevOperation : undefined}
      >
        <IconSVG name="icon-caret-left" />
      </span>
      <span>{activeOpsIndex + 1}</span>
      <span className="separator">of</span>
      <span>{limit}</span>
      <span
        className={classnames('nav-icon', { disabled: nextDisabled })}
        onClick={!nextDisabled ? nextOperation : undefined}
      >
        <IconSVG name="icon-caret-right" />
      </span>
    </div>
  );
};

export default NavigationView;
