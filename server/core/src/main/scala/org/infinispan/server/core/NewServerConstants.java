package org.infinispan.server.core;

import javax.security.auth.Subject;

/**
 * // TODO: Document this
 *
 * @author wburns
 * @since 4.0
 */
public class NewServerConstants {
   private NewServerConstants() { }

   public static final int EXPIRATION_NONE = -1;
   public static final int EXPIRATION_DEFAULT = -2;

   public static final Subject ANONYMOUS = new Subject();
}
