package org.infinispan.server.router.router;

import java.net.InetAddress;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.router.RoutingTable;

/**
 * The EndpointRouter interface. Currently the EndpointRouter is coupled closely with the the {@link Protocol} it implements.
 *
 * @author Sebastian Łaskawiec
 */
public interface EndpointRouter {

    /**
     * The protocol the router implements.
     */
    enum Protocol {
        HOT_ROD, SINGLE_PORT, REST
    }

    /**
     * Starts the {@link EndpointRouter}.
     *
     * @param routingTable {@link RoutingTable} for supplying {@link org.infinispan.server.router.routes.Route}s.
     */
    void start(RoutingTable routingTable, EmbeddedCacheManager cm);

    /**
     * Stops the {@link EndpointRouter}.
     */
    void stop();

    /**
     * Gets the {@link EndpointRouter}'s host.
     */
    String getHost();

    /**
     * Gets the {@link EndpointRouter}'s IP address.
     */
    InetAddress getIp();

    /**
     * Gets the {@link EndpointRouter}'s port.
     */
    Integer getPort();

    /**
     * Gets {@link Protocol} implemented by this {@link EndpointRouter}.
     */
    Protocol getProtocol();

}
