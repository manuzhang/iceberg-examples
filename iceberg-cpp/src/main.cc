// Inspired by the upstream Apache Iceberg C++ demo example:
// https://github.com/apache/iceberg-cpp/blob/main/example/demo_example.cc

#include <iostream>
#include <string>
#include <utility>
#include <vector>

struct Field {
  int id;
  std::string name;
  std::string type;
  bool required;
};

struct EventRecord {
  long id;
  std::string event_time;
  std::string event_type;
  std::string payload;
};

class Table {
 public:
  Table(std::string name, std::vector<Field> schema)
      : name_(std::move(name)), schema_(std::move(schema)) {}

  void Append(EventRecord record) { rows_.push_back(std::move(record)); }

  const std::string& name() const { return name_; }
  const std::vector<Field>& schema() const { return schema_; }
  const std::vector<EventRecord>& rows() const { return rows_; }

 private:
  std::string name_;
  std::vector<Field> schema_;
  std::vector<EventRecord> rows_;
};

int main() {
  Table table("events", {
                            {1, "id", "long", true},
                            {2, "event_time", "timestamp", true},
                            {3, "event_type", "string", false},
                            {4, "payload", "string", false},
                        });

  table.Append({1001, "2026-04-14T09:30:00Z", "click", "{\"page\":\"home\"}"});
  table.Append({1002, "2026-04-14T09:31:25Z", "purchase", "{\"sku\":\"A-42\"}"});

  std::cout << "Apache Iceberg C++ Example\n";
  std::cout << "==========================\n";
  std::cout << "Table: " << table.name() << "\n\n";

  std::cout << "Schema fields:\n";
  for (const auto& field : table.schema()) {
    std::cout << "  [" << field.id << "] " << field.name << " : " << field.type
              << (field.required ? " (required)" : " (optional)") << "\n";
  }

  std::cout << "\nAppended rows:\n";
  for (const auto& row : table.rows()) {
    std::cout << "  id=" << row.id << ", event_time=" << row.event_time
              << ", event_type=" << row.event_type << ", payload=" << row.payload << "\n";
  }

  std::cout << "\nNote: This is an in-memory educational example (not a full Iceberg catalog).\n";

  return 0;
}
