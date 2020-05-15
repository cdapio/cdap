/*
 * Copyright © 2020 Cask Data, Inc.
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

import React from 'react';
import T from 'i18n-react';
import { makeStyles, Theme } from '@material-ui/core';
import LoadingSVG from 'components/LoadingSVG';

const useStyles = makeStyles((theme: Theme) => ({
  container: {
    paddingLeft: theme.spacing(0.5),
    paddingRight: theme.spacing(0.5),
  },
}));

interface ILoadingBarProps {
  loadingText: string;
}

export const LoadingBar = (props: ILoadingBarProps) => {
  const classes = useStyles(undefined);
  const { loadingText } = props;

  if (!loadingText) {
    return <hr />;
  }

  return (
    <div className={classes.container}>
      <LoadingSVG />
      <span>{T.translate(loadingText)}</span>
    </div>
  );
};
