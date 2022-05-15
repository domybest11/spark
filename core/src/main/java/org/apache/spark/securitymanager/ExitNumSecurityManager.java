package org.apache.spark.securitymanager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.Permission;

public class ExitNumSecurityManager extends SecurityManager {
    private final static Logger LOGGER = LoggerFactory.getLogger(ExitNumSecurityManager.class);

    public SecurityManager parentSecurityManager;

    public ExitNumSecurityManager(SecurityManager parent) {
        super();
        LOGGER.info("parentSecurityManager is {}", parent);
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