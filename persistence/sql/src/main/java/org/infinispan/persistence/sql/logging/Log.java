package org.infinispan.persistence.sql.logging;

import static org.jboss.logging.Logger.Level.ERROR;
import static org.jboss.logging.Logger.Level.WARN;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import org.jboss.logging.BasicLogger;
import org.jboss.logging.Logger;
import org.jboss.logging.annotations.Cause;
import org.jboss.logging.annotations.LogMessage;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;

@MessageLogger(projectCode = "ISPN")
public interface Log extends BasicLogger {
   Log CONFIG = Logger.getMessageLogger(Log.class, org.infinispan.util.logging.Log.LOG_ROOT + "CONFIG");
   Log PERSISTENCE = Logger.getMessageLogger(Log.class, org.infinispan.util.logging.Log.LOG_ROOT + "PERSISTENCE");

   @LogMessage(level = ERROR)
   @Message(value = "Exception while marshalling object: %s", id = 65)
   void errorMarshallingObject(@Cause Throwable ioe, Object obj);

   @LogMessage(level = ERROR)
   @Message(value = "I/O error while unmarshalling from stream", id = 8009)
   void ioErrorUnmarshalling(@Cause IOException e);

   @LogMessage(level = ERROR)
   @Message(value = "*UNEXPECTED* ClassNotFoundException.", id = 8010)
   void unexpectedClassNotFoundException(@Cause ClassNotFoundException e);

   @LogMessage(level = ERROR)
   @Message(value = "Issues while closing connection %s", id = 8019)
   void sqlFailureClosingConnection(Connection conn, @Cause SQLException e);

   @LogMessage(level = WARN)
   @Message(value = "Unexpected sql failure", id = 8022)
   void sqlFailureUnexpected(@Cause SQLException e);
}
