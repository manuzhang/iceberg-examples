package org.apache.iceberg.rest;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

public final class EmbeddedRestCatalogServer implements AutoCloseable {
  private final RESTCatalogServer server;
  private final Server jdbcServer;
  private final JdbcCatalog jdbcCatalog;
  private final String catalogUri;

  private EmbeddedRestCatalogServer(
      RESTCatalogServer server, Server jdbcServer, JdbcCatalog jdbcCatalog, String catalogUri) {
    this.server = server;
    this.jdbcServer = jdbcServer;
    this.jdbcCatalog = jdbcCatalog;
    this.catalogUri = catalogUri;
  }

  public static EmbeddedRestCatalogServer noop() {
    return new EmbeddedRestCatalogServer(null, null, null, null);
  }

  public static boolean isReachable(String catalogUri) {
    try {
      URL configUrl = URI.create(catalogUri + "/v1/config").toURL();
      HttpURLConnection connection = (HttpURLConnection) configUrl.openConnection();
      connection.setConnectTimeout(500);
      connection.setReadTimeout(500);
      connection.setRequestMethod("GET");
      connection.getResponseCode();
      connection.disconnect();
      return true;
    } catch (IOException e) {
      return false;
    }
  }

  public static EmbeddedRestCatalogServer start(String catalogUri, String warehousePath)
      throws Exception {
    Map<String, String> config = new HashMap<>();
    config.put(CatalogProperties.CATALOG_IMPL, HadoopCatalog.class.getName());
    config.put(CatalogProperties.WAREHOUSE_LOCATION, warehousePath);
    return start(catalogUri, config);
  }

  public static EmbeddedRestCatalogServer startJdbcSqliteInMemoryFileIO(
      String catalogUri, String jdbcUri, String warehousePath) throws Exception {
    URI uri = URI.create(catalogUri);
    JdbcCatalog catalog = new JdbcCatalog();
    catalog.initialize(
        "rest_backend",
        Map.of(
            CatalogProperties.URI, jdbcUri,
            CatalogProperties.FILE_IO_IMPL, InMemoryFileIO.class.getName(),
            CatalogProperties.WAREHOUSE_LOCATION, warehousePath,
            "jdbc.schema-version", "V1"));

    RESTCatalogAdapter adapter = new RESTCatalogAdapter(catalog);
    RESTCatalogServlet servlet = new RESTCatalogServlet(adapter);

    Server server = new Server();
    ServerConnector connector = new ServerConnector(server);
    connector.setHost(uri.getHost());
    connector.setPort(uri.getPort());
    server.addConnector(connector);

    ServletContextHandler context = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
    context.setContextPath("/");
    context.addServlet(new ServletHolder(servlet), "/*");
    server.setHandler(context);
    server.start();

    String resolvedCatalogUri =
        "http://" + connector.getHost() + ":" + connector.getLocalPort();
    return new EmbeddedRestCatalogServer(null, server, catalog, resolvedCatalogUri);
  }

  private static EmbeddedRestCatalogServer start(String catalogUri, Map<String, String> config)
      throws Exception {
    URI uri = URI.create(catalogUri);
    int port = uri.getPort() == -1 ? RESTCatalogServer.REST_PORT_DEFAULT : uri.getPort();

    Map<String, String> mergedConfig = new HashMap<>(config);
    mergedConfig.put(RESTCatalogServer.REST_PORT, String.valueOf(port));

    RESTCatalogServer server = new RESTCatalogServer(mergedConfig);
    server.start(false);
    return new EmbeddedRestCatalogServer(server, null, null, catalogUri);
  }

  public String catalogUri() {
    return catalogUri;
  }

  @Override
  public void close() throws Exception {
    if (server != null) {
      server.stop();
    }
    if (jdbcServer != null) {
      jdbcServer.stop();
    }
    if (jdbcCatalog != null) {
      jdbcCatalog.close();
    }
  }
}
