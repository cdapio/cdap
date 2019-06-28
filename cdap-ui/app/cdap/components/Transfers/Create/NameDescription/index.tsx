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
import withStyles, { WithStyles, StyleRules } from '@material-ui/core/styles/withStyles';
import { transfersCreateConnect } from 'components/Transfers/Create/context';
import TextField from '@material-ui/core/TextField';
import StepButtons from 'components/Transfers/Create/StepButtons';

const styles = (): StyleRules => {
  return {
    root: {
      '& > div': {
        marginBottom: '25px',
      },
    },
  };
};

interface INameDescriptionProps extends WithStyles<typeof styles> {
  setNameDescription: (name: string, description: string) => void;
  name: string;
  description: string;
}

const NameDescriptionView: React.SFC<INameDescriptionProps> = ({
  name,
  description,
  classes,
  setNameDescription,
}) => {
  const [localName, setName] = React.useState(name);
  const [localDescription, setDescription] = React.useState(description);

  return (
    <div className={classes.root}>
      <TextField
        fullWidth
        label="name"
        placeholder="Set a name for this data transfer"
        variant="outlined"
        value={localName}
        onChange={(e) => setName(e.target.value)}
      />
      <TextField
        label="Description"
        placeholder="Write a short description (240 characters max)"
        multiline
        fullWidth
        rows="5"
        variant="outlined"
        value={localDescription}
        onChange={(e) => setDescription(e.target.value)}
      />
      <StepButtons onNext={setNameDescription.bind(null, localName, localDescription)} />
    </div>
  );
};

const StyledNameDescription = withStyles(styles)(NameDescriptionView);
const NameDescription = transfersCreateConnect(StyledNameDescription);
export default NameDescription;
