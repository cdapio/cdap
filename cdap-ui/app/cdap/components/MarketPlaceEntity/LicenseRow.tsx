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
import T from 'i18n-react';

interface ILicenseRowProps {
  licenseInfo?: {
    name: string;
    url?: string;
  };
}

interface ITagProps {
  href?: string;
  target?: '_blank';
}

const LicenseRow: React.SFC<ILicenseRowProps> = ({ licenseInfo }) => {
  if (!licenseInfo) {
    return null;
  }

  const Tag = licenseInfo.url ? 'a' : 'span';
  const tagProps: ITagProps = {};

  if (licenseInfo.url) {
    tagProps.href = licenseInfo.url;
    tagProps.target = '_blank';
  }

  return (
    <div>
      <span>
        <strong> {T.translate('features.MarketPlaceEntity.Metadata.licenseInfo')} </strong>
      </span>
      <Tag {...tagProps}>{licenseInfo.name}</Tag>
    </div>
  );
};

export default LicenseRow;
