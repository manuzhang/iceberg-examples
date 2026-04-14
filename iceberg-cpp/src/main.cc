// For a full native Iceberg C++ workflow example, see
// https://github.com/apache/iceberg-cpp/blob/main/example/demo_example.cc

#include <iostream>
#include <string>
#include <vector>

struct Field {
  int id;
  std::string name;
  std::string type;
  bool required;
};

int main() {
  const std::vector<Field> schema = {
      {1, "id", "long", true},
      {2, "event_time", "timestamp", true},
      {3, "event_type", "string", false},
      {4, "payload", "string", false},
  };

  std::cout << "Apache Iceberg C++ Example\n";
  std::cout << "==========================\n";
  std::cout << "This example builds an Iceberg-style schema model in C++.\n\n";

  std::cout << "Schema fields:\n";
  for (const auto& field : schema) {
    std::cout << "  [" << field.id << "] " << field.name << " : " << field.type
              << (field.required ? " (required)" : " (optional)") << "\n";
  }

  std::cout << "\nNote: Apache Iceberg does not yet publish an official native C++ API.\n";
  std::cout << "C++ workflows commonly interoperate through Arrow/Parquet files and\n";
  std::cout << "Iceberg metadata produced by Java, Spark, Rust, or Python engines.\n";

  return 0;
}
