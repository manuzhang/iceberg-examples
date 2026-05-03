package io.github.manuzhang.iceberg.examples;

import org.apache.iceberg.inmemory.InMemoryFileIO;

/**
 * Keeps the example-local in-memory warehouse alive across short-lived catalog instances.
 *
 * <p>Beam and REST clients may create and close multiple FileIO instances during a single example
 * run. {@link InMemoryFileIO} stores file contents in a shared static map, so closing one instance
 * should not invalidate the warehouse for the rest of the process.
 */
public class SharedInMemoryFileIO extends InMemoryFileIO {

  @Override
  public void close() {
    // Leave the shared in-memory warehouse available for the rest of the example process.
  }
}
