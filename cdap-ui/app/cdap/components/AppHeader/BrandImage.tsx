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
import withStyles, { WithStyles } from '@material-ui/core/styles/withStyles';
import { withContext, INamespaceLinkContext } from 'components/AppHeader/NamespaceLinkContext';
import { Theme } from 'services/ThemeHelper';
import { Link } from 'react-router-dom';
import { getCurrentNamespace } from 'services/NamespaceStore';

interface IBrandImageProps extends WithStyles<typeof imageStyle> {
  context: INamespaceLinkContext;
}

const BrandImage: React.SFC<IBrandImageProps> = ({ classes, context }) => {
  const brandLogoSrc = Theme.productLogoNavbar || '/cdap_assets/img/company_logo-20-all.png';
  const { isNativeLink } = context;
  const namespace = getCurrentNamespace();
  const LinkEl = isNativeLink ? 'a' : Link;
  return (
    <LinkEl to={`/ns/${namespace}`} href={`/cdap/ns/${namespace}`}>
      <img className={classes.img} src={brandLogoSrc} />
    </LinkEl>
  );
};

const imageStyle = {
  img: {
    height: '48px',
  },
};
const StyledBrandImage = withStyles(imageStyle)(withContext(BrandImage));
export default StyledBrandImage;
