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

import * as React from 'react';
import Button from '@material-ui/core/Button';
import { IWidgetProps } from 'components/AbstractWidget';
import { objectQuery } from 'services/helpers';

interface IGetSchemaProps extends IWidgetProps<null> {}

const GetSchemaWidget: React.FC<IGetSchemaProps> = ({ extraConfig }) => {
  const validateProperties = objectQuery(extraConfig, 'validateProperties');

  return (
    <div>
      <Button
        variant="contained"
        color="primary"
        disabled={typeof validateProperties !== 'function'}
        onClick={validateProperties}
      >
        Get Schema
      </Button>
    </div>
  );
};

export default GetSchemaWidget;
