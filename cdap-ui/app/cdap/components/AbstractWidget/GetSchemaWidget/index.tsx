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

import Button from '@material-ui/core/Button';
import withStyles, { StyleRules, WithStyles } from '@material-ui/core/styles/withStyles';
import { IWidgetProps } from 'components/AbstractWidget';
import IconSVG from 'components/IconSVG';
import * as React from 'react';
import { objectQuery } from 'services/helpers';

const styles = (): StyleRules => {
  return {
    button: {
      width: '105px',
      height: '30px',
    },
    spinner: {
      fontSize: '16px',
    },
    buttonWrapper: {
      textAlign: 'right',
    },
  };
};

export enum Position {
  TopLeft = 'top-left',
  TopRight = 'top-right',
  BottomLeft = 'bottom-left',
  BottomRight = 'bottom-right',
}
interface IGetSchemaWidgetProps {
  position?: Position;
}

interface IGetSchemaProps extends IWidgetProps<IGetSchemaWidgetProps>, WithStyles<typeof styles> {}

const GetSchemaWidgetView: React.FC<IGetSchemaProps> = ({ extraConfig, classes, widgetProps }) => {
  const validateProperties = objectQuery(extraConfig, 'validateProperties');
  const [loading, setLoading] = React.useState<boolean>(false);
  const position = widgetProps.position || '';

  function onClickHander() {
    if (loading) {
      return;
    }

    if (validateProperties && typeof validateProperties === 'function') {
      setLoading(true);
      validateProperties(() => {
        setLoading(false);
      }, true);
    }
  }

  const loadingIcon = (
    <span className={`fa fa-spin ${classes.spinner}`}>
      <IconSVG name="icon-spinner" />
    </span>
  );

  const className =
    position === Position.TopRight || position === Position.BottomRight
      ? classes.buttonWrapper
      : '';
  return (
    <div className={className}>
      <Button
        variant="outlined"
        color="default"
        disabled={typeof validateProperties !== 'function'}
        onClick={onClickHander}
        className={classes.button}
        data-cy="get-schema-btn"
      >
        {loading ? loadingIcon : 'Get Schema'}
      </Button>
    </div>
  );
};

const GetSchemaWidget = withStyles(styles)(GetSchemaWidgetView);
export default GetSchemaWidget;

(GetSchemaWidget as any).getWidgetAttributes = () => {
  return {
    position: { type: 'Position', required: false },
  };
};
