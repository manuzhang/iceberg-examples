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

public final class EmbeddedRestCatalogServer implements AutoCloseable {
  private final RESTCatalogServer server;

  private EmbeddedRestCatalogServer(RESTCatalogServer server) {
    this.server = server;
  }

  public static EmbeddedRestCatalogServer noop() {
    return new EmbeddedRestCatalogServer(null);
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
    Map<String, String> config = new HashMap<>();
    config.put(CatalogProperties.CATALOG_IMPL, JdbcCatalog.class.getName());
    config.put(CatalogProperties.URI, jdbcUri);
    config.put(CatalogProperties.FILE_IO_IMPL, InMemoryFileIO.class.getName());
    config.put(CatalogProperties.WAREHOUSE_LOCATION, warehousePath);
    return start(catalogUri, config);
  }

  private static EmbeddedRestCatalogServer start(String catalogUri, Map<String, String> config)
      throws Exception {
    URI uri = URI.create(catalogUri);
    int port = uri.getPort() == -1 ? RESTCatalogServer.REST_PORT_DEFAULT : uri.getPort();

    Map<String, String> mergedConfig = new HashMap<>(config);
    mergedConfig.put(RESTCatalogServer.REST_PORT, String.valueOf(port));

    RESTCatalogServer server = new RESTCatalogServer(mergedConfig);
    server.start(false);
    return new EmbeddedRestCatalogServer(server);
  }

  @Override
  public void close() throws Exception {
    if (server != null) {
      server.stop();
    }
  }
}
