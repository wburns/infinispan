package org.infinispan.commons.dataconversion;

/**
 * @since 9.2
 */
public interface EncoderIds {

   short IDENTITY = 1;
   short BINARY = 2;
   short UTF8 = 3;
   short GLOBAL_MARSHALLER = 4;
   short GENERIC_MARSHALLER = 5;
   short JAVA_SERIALIZATION = 6;
}
