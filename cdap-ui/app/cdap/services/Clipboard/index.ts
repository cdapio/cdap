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

function useDocumentExecPaste() {
  const promise = new Promise((resolve, reject) => {
    const textarea = document.createElement('textarea');
    textarea.style.position = 'fixed';
    textarea.style.top = '0';
    textarea.style.left = '0';
    textarea.style.width = '2em';
    textarea.style.height = '2em';
    textarea.style.padding = '0';
    textarea.style.border = 'none';
    textarea.style.outline = 'none';
    textarea.style.boxShadow = 'none';
    textarea.style.background = 'transparent';
    document.body.appendChild(textarea);
    textarea.focus();

    try {
      document.execCommand('paste');
    } catch (e) {
      reject('Unable to paste from clipboard: ' + e.message);
    }
    resolve(textarea.textContent);
    document.body.removeChild(textarea);
  });
  return promise;
}

function useDocumentExecCopy(text: string) {
  const promise = new Promise((resolve, reject) => {
    const textarea = document.createElement('textarea');
    textarea.value = text;
    textarea.style.position = 'fixed';
    textarea.style.top = '0';
    textarea.style.left = '0';
    textarea.style.width = '2em';
    textarea.style.height = '2em';
    textarea.style.padding = '0';
    textarea.style.border = 'none';
    textarea.style.outline = 'none';
    textarea.style.boxShadow = 'none';
    textarea.style.background = 'transparent';
    textarea.value = text;
    document.body.appendChild(textarea);
    textarea.value = text;
    textarea.select();

    try {
      document.execCommand('copy');
    } catch (e) {
      reject('Unable to copy: ' + e.message);
    }
    document.body.removeChild(textarea);
    resolve();
  });
  return promise;
}

export async function copyToClipBoard(text) {
  // @ts-ignore
  if (!navigator.clipboard) {
    return await useDocumentExecCopy(text);
  }

  try {
    // @ts-ignore
    await navigator.clipboard.writeText(text);
    return Promise.resolve();
  } catch (e) {
    /* tslint:disable:no-console */
    console.error('Unable to copy: ' + e.message);
    return await useDocumentExecCopy(text);
  }
}

export async function getValueFromClipboard() {
  // @ts-ignore
  if (!navigator.clipboard) {
    return await useDocumentExecPaste();
  }

  try {
    // @ts-ignore
    const clipText = await navigator.clipboard.readText();
    return Promise.resolve(clipText);
  } catch (e) {
    /* tslint:disable:no-console */
    console.error('Unable to copy from clipboard: ' + e.message);
    return await useDocumentExecPaste();
  }
}
