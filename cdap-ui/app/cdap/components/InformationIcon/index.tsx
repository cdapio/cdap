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
import Proptypes from 'prop-types';
import React from 'react';
import { UncontrolledTooltip } from 'reactstrap';
import IconSVG from 'components/IconSVG';
import uuidV4 from 'uuid/v4';
import withStyles, { WithStyles } from '@material-ui/core/styles/withStyles';
import ThemeWrapper from 'components/ThemeWrapper';

/**
 * Right now this component is not being used anywhere.
 * Keeping it still here because we would easily come across a usecase where we need
 * to show a info icon with a tooltip and this could come in handy.
 */
const styles = (theme) => {
  return {
    root: {
      display: 'flex',
    },
    svgIconStyles: {
      color: theme.palette.blue['100'],
    },
  };
};
interface IInformationIconprops extends WithStyles<typeof styles> {
  description?: string;
}
function InformationIcon({ description, classes }: IInformationIconprops) {
  if (!description || (typeof description === 'string' && description === '')) {
    return null;
  }
  const iconId = `group-desc-${uuidV4()}`;
  return (
    <span className={classes.root}>
      <IconSVG id={iconId} name="icon-info-circle" className={classes.svgIconStyles} />
      <UncontrolledTooltip target={iconId} placement="top">
        {description}
      </UncontrolledTooltip>
    </span>
  );
}

function InformationIconWrapper({ description }) {
  const StyledInformationIcon = withStyles(styles)(InformationIcon);
  return (
    <ThemeWrapper>
      <StyledInformationIcon description={description} />
    </ThemeWrapper>
  );
}

(InformationIconWrapper as any).propTypes = {
  description: Proptypes.string,
};
export default InformationIconWrapper;
