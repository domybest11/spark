package org.apache.spark.securitymanager;

import java.security.Permission;

public class ExitNumSecurityManager extends SecurityManager {

    public SecurityManager parentSecurityManager;

    public ExitNumSecurityManager(SecurityManager parent) {
        super();
        parentSecurityManager = parent;
    }

    @Override
    public void checkPermission(Permission perm, Object context) {
        if (parentSecurityManager != null) {
            parentSecurityManager.checkPermission(perm, context);
        }
    }

    @Override
    public void checkPermission(Permission perm) {
        if (parentSecurityManager != null) {
            parentSecurityManager.checkPermission(perm);
        }
    }

    @Override
    public void checkExit(int status) {
        throw new ExitException(status);
    }

    public static class ExitException extends RuntimeException {
        int status;

        public ExitException(int status) {
            this.status = status;
        }

        public int getStatus() {
            return status;
        }
    }
}