package com.continuuity.flow.devtools.eclipse;

import org.eclipse.core.runtime.CoreException;
import org.eclipse.core.runtime.IStatus;
import org.eclipse.core.runtime.Status;
import org.eclipse.jface.dialogs.ErrorDialog;
import org.eclipse.ui.internal.Workbench;
import org.eclipse.ui.internal.WorkbenchPlugin;


/**
 * A utility class to provide exception handling.
 */
public class ExceptionHandler {

  /**
   * Throws a new {@link CoreException} generated from the given exception.
   * 
   * @param message the message for the {@link Status} object
   * @param exception the exception to be converted to {@link CoreException}
   * @throws CoreException
   */
  public static void throwCoreException(String message, Exception exception) throws CoreException {
    IStatus status =
        new Status(IStatus.ERROR, "com.continuuity.flow", IStatus.OK, message, exception);
    throw new CoreException(status);
  }
  
  /**
   * Returns a new {@link CoreException} generated from the given exception.
   * 
   * @param message the message for the {@link Status} object
   * @param exception the exception to be converted to {@link CoreException}
   * @return  A {@link CoreException} generated from the given exception
   */
  public static CoreException getNewCoreException(String message, Exception exception) {
    IStatus status =
        new Status(IStatus.ERROR, "com.continuuity.flow", IStatus.OK, message, exception);
    return new CoreException(status);
  }
  
  /**
   * Opens an error dialog with an error message and logs the error 
   * to the default log.
   * 
   * @param title the title of the error dialog
   * @param message the message to be displayed in the error dialog
   * @param coreException the {@link CoreException} to be logged
   */
  public static void handleCoreException(String title, String message, 
      CoreException coreException) {
    ErrorDialog.openError(Workbench.getInstance().getActiveWorkbenchWindow().getShell(),
        title, message, coreException.getStatus());
    WorkbenchPlugin.log(coreException.getStatus());
  }
  
}
