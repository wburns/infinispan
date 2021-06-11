package org.infinispan.persistence.jdbc.common.configuration;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.persistence.jdbc.common.connectionfactory.ConnectionFactory;

/**
 * ConnectionFactoryConfiguration.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public interface ConnectionFactoryConfiguration extends ConfigurationInfo {
   Class<? extends ConnectionFactory> connectionFactoryClass();
}
