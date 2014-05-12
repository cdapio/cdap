package com.continuuity.security.server;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import java.security.Principal;



/* ---------------------------------------------------- */
/** JAASUserPrincipal
 * <p>Implements the JAAS version of the
 *  org.eclipse.jetty.http.UserPrincipal interface.
 *
 * @version $Id: JAASUserPrincipal.java 4780 2009-03-17 15:36:08Z jesse $
 *
 */
public class JAASUserPrincipal implements Principal {
  private final String _name;
  private final Subject _subject;
  private final LoginContext _loginContext;

    /* ------------------------------------------------ */

  public JAASUserPrincipal(String name, Subject subject, LoginContext loginContext) {
    this._name = name;
    this._subject = subject;
    this._loginContext = loginContext;
  }

    /* ------------------------------------------------ */
  /** Get the name identifying the user
   */
  public String getName () {
    return _name;
  }


    /* ------------------------------------------------ */
  /** Provide access to the Subject
   * @return subject
   */
  public Subject getSubject () {
    return this._subject;
  }

  public LoginContext getLoginContext () {
    return this._loginContext;
  }

  public String toString() {
    return getName();
  }

}

