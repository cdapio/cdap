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
import NavLinkWrapper from 'components/NavLinkWrapper';
import { Theme } from 'services/ThemeHelper';
import { withContext } from 'components/Header/NamespaceLinkContext';
require('./BrandSection.scss');

interface IBrandSectionProps {
  context: {
    namespace: string;
    isNativeLink: boolean;
  };
}

const BrandSection: React.SFC<IBrandSectionProps> = ({ context }) => {
  const { namespace, isNativeLink } = context;
  const baseCDAPUrl = `/ns/${namespace}`;
  const brandLogoSrc = Theme.productLogoNavbar || '/cdap_assets/img/company_logo.png';
  return (
    <div className="brand-section">
      <NavLinkWrapper
        isNativeLink={isNativeLink}
        to={isNativeLink ? `/cdap${baseCDAPUrl}` : baseCDAPUrl}
      >
        <img src={brandLogoSrc} />
      </NavLinkWrapper>
    </div>
  );
};

const BrandSectionWithContext = withContext(BrandSection);

export default BrandSectionWithContext;
