package org.infinispan.rest.logging;

import io.netty.channel.ChannelHandlerContext;
import org.infinispan.util.logging.LogFactory;
import org.jboss.resteasy.spi.ResteasyProviderFactory;
import org.jboss.resteasy.util.DateUtil;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.ext.Provider;
import java.io.IOException;
import java.util.Date;
import java.util.Map;

/**
 * Logging filter that can be used to output requests in a similar fashion to HTTPD log output
 *
 * @author wburns
 * @since 9.0
 */
@Provider
public class LoggingFilter implements ContainerResponseFilter, ContainerRequestFilter {
   private final static JavaLog log = LogFactory.getLog(LoggingFilter.class, JavaLog.class);

   @Context
   private ChannelHandlerContext context;

   @Override
   public void filter(ContainerRequestContext requestContext, ContainerResponseContext responseContext) throws IOException {
      Map<Class<?>, Object> map = ResteasyProviderFactory.getContextDataMap();
      Object nonProxyContext = map.get(ChannelHandlerContext.class);
      // IP
      String remoteAddress = context.channel().remoteAddress().toString();
      // Date
      Date requestDate = requestContext.getDate();
      // Request method | path | protocol
      String requestMethod = requestContext.getMethod();
      String uri = requestContext.getUriInfo().getPath();
      // Status code
      int status = responseContext.getStatus();
      // Size
      int length = -1;
      // Response time
      long responseTime = System.currentTimeMillis() - requestDate.getTime();

      log.fatalf("%s [%s] \"%s %s\" %s %s %s ms", remoteAddress, requestDate, requestMethod, uri, status, length,
              responseTime);
   }

   @Override
   public void filter(ContainerRequestContext requestContext) throws IOException {
      if (requestContext.getDate() == null) {
         // Make sure the date header is set
         requestContext.getHeaders().putSingle(HttpHeaders.DATE, DateUtil.formatDate(new Date()));
      }
   }
}
