package com.weibo.dip.platform.hessian;

import com.weibo.dip.platform.util.ApplicationTimerTask;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServiceClient {
  private static final Logger LOGGER = LoggerFactory.getLogger(ServiceClient.class);

  public static void main(String[] args) throws Exception {
    new ApplicationTimerTask();
    Server server = new Server(8088);
    ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
    server.setHandler(context);
    ServletHolder servletHolder = new ServletHolder(new ClientServiceImpl());
    context.addServlet(servletHolder, "/service/clientservice");
    server.start();
    LOGGER.info("hessian start!");
  }
}
