import isNil from 'lodash/isNil';
import cookie from 'react-cookie';
export const USE_REMOTE_SERVER = false;
export const REMOTE_IP = "https://platacc001-mst-01.cloud.in.guavus.com:10443";
export const RAF_ACCESS_TOKEN = "Agp1c3IwMQCs0NzvlVuswI/ClluwuMGmC0C1B07J4qgUqVTQQFN67O/6tK8ptiyE10qYYTgGXfxMPA==";


export function getDefaultRequestHeader() {
    if (USE_REMOTE_SERVER) {
      return {
        "AccessToken": `Bearer ${RAF_ACCESS_TOKEN}`
      };
    } else {
      return (isNil(cookie.load('CDAP_Auth_Token'))) ? {} : { "AccessToken": `Bearer ${cookie.load('CDAP_Auth_Token')}` };
    }
  }
