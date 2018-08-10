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
import { Theme } from 'services/ThemeHelper';
import classnames from 'classnames';
import MetadataDropdown from 'components/Header/MetadataDropdown';

export default function MetadataLink() {
  if (Theme.showMetadata === false) {
    return null;
  }

  const isMetadataActive = location.pathname.indexOf('metadata') !== -1;
  return (
    <li className={classnames({
      active: isMetadataActive,
    })}>
      <MetadataDropdown />
    </li>
  );
}
