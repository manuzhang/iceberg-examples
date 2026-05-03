package org.apache.iceberg.rest;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import io.github.manuzhang.iceberg.examples.SharedInMemoryFileIO;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

public final class EmbeddedRestCatalogServer implements AutoCloseable {
  private static final int DEFAULT_REST_PORT = 8181;
  private static final String DEFAULT_BACKEND_CATALOG_NAME = "rest_backend";

  private final Server server;
  private final JdbcCatalog backendCatalog;
  private final String catalogUri;
  private final Path temporaryCatalogDir;

  private EmbeddedRestCatalogServer(
      Server server, JdbcCatalog backendCatalog, String catalogUri, Path temporaryCatalogDir) {
    this.server = server;
    this.backendCatalog = backendCatalog;
    this.catalogUri = catalogUri;
    this.temporaryCatalogDir = temporaryCatalogDir;
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
    Path temporaryCatalogDir = Files.createTempDirectory("iceberg-rest-catalog");
    Map<String, String> config = new HashMap<>();
    config.put(CatalogProperties.URI, sqliteCatalogUri(temporaryCatalogDir));
    config.put(CatalogProperties.WAREHOUSE_LOCATION, warehousePath);
    config.put("jdbc.schema-version", "V1");
    if (usesInMemoryFileIO(warehousePath)) {
      config.put(CatalogProperties.FILE_IO_IMPL, SharedInMemoryFileIO.class.getName());
    }
    return start(catalogUri, config, temporaryCatalogDir);
  }

  public static EmbeddedRestCatalogServer startJdbcSqliteInMemoryFileIO(
      String catalogUri, String jdbcUri, String warehousePath) throws Exception {
    Map<String, String> config = new HashMap<>();
    config.put(CatalogProperties.URI, jdbcUri);
    config.put(CatalogProperties.FILE_IO_IMPL, SharedInMemoryFileIO.class.getName());
    config.put(CatalogProperties.WAREHOUSE_LOCATION, warehousePath);
    config.put("jdbc.schema-version", "V1");
    return start(catalogUri, config, null);
  }

  private static EmbeddedRestCatalogServer start(
      String catalogUri, Map<String, String> config, Path temporaryCatalogDir) throws Exception {
    URI uri = URI.create(catalogUri);
    int port = uri.getPort() == -1 ? DEFAULT_REST_PORT : uri.getPort();

    JdbcCatalog backendCatalog = new JdbcCatalog();
    backendCatalog.initialize(DEFAULT_BACKEND_CATALOG_NAME, config);

    RESTCatalogAdapter adapter = new RESTCatalogAdapter(backendCatalog);
    RESTCatalogServlet servlet = new RESTCatalogServlet(adapter);
    Server server = new Server();
    ServerConnector connector = new ServerConnector(server);
    connector.setPort(port);
    if (uri.getHost() != null && !uri.getHost().isBlank()) {
      connector.setHost(uri.getHost());
    }
    server.addConnector(connector);

    ServletContextHandler context = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
    context.setContextPath("/");
    context.addServlet(new ServletHolder(servlet), "/*");
    server.setHandler(context);

    try {
      server.start();
      return new EmbeddedRestCatalogServer(
          server, backendCatalog, resolvedCatalogUri(connector), temporaryCatalogDir);
    } catch (Exception e) {
      backendCatalog.close();
      deleteRecursively(temporaryCatalogDir);
      throw e;
    }
  }

  public String catalogUri() {
    return catalogUri;
  }

  @Override
  public void close() throws Exception {
    Exception stopFailure = null;

    if (server != null) {
      try {
        server.stop();
      } catch (Exception e) {
        stopFailure = e;
      }
    }

    if (backendCatalog != null) {
      backendCatalog.close();
    }

    deleteRecursively(temporaryCatalogDir);

    if (stopFailure != null) {
      throw stopFailure;
    }
  }

  private static boolean usesInMemoryFileIO(String warehousePath) {
    return warehousePath.startsWith("in-memory://");
  }

  private static String sqliteCatalogUri(Path catalogDir) {
    return "jdbc:sqlite:" + catalogDir.resolve("catalog.db").toAbsolutePath();
  }

  private static String resolvedCatalogUri(ServerConnector connector) {
    String host = connector.getHost();
    if (host == null || host.isBlank()) {
      host = "localhost";
    }
    return "http://" + host + ":" + connector.getLocalPort();
  }

  private static void deleteRecursively(Path root) {
    if (root == null) {
      return;
    }

    try (Stream<Path> paths = Files.walk(root)) {
      paths.sorted(Comparator.reverseOrder())
          .forEach(
              path -> {
                try {
                  Files.deleteIfExists(path);
                } catch (IOException e) {
                  throw new RuntimeException("Unable to delete " + path, e);
                }
              });
    } catch (IOException e) {
      throw new RuntimeException("Unable to clean up " + root, e);
    }
  }
}
