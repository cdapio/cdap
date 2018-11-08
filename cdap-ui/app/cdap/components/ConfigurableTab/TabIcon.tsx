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
import { objectQuery } from 'services/helpers';
import IconSVG from 'components/IconSVG';

interface IIcon {
  type: 'font-icon' | 'link' | 'inline';
  arguments: {
    data?: string;
    url: string;
  };
}

interface ITabIconProps {
  iconObj?: IIcon;
}

const TabIcon: React.SFC<ITabIconProps> = ({ iconObj }) => {
  if (!iconObj) {
    return null;
  }

  if (iconObj.type === 'font-icon') {
    const iconName = objectQuery(iconObj, 'arguments', 'data');
    return (
      <span className="fa-fw tab-header-icon">
        <IconSVG name={iconName} />
      </span>
    );
  } else if (iconObj.type === 'link' || iconObj.type === 'inline') {
    const property = iconObj.type === 'link' ? 'url' : 'data';
    const iconSrc = objectQuery(iconObj, 'arguments', property);
    return (
      <span className="tab-header-icon link">
        <img src={iconSrc} />
      </span>
    );
  }

  return null;
};

export default TabIcon;
