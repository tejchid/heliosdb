#pragma once

#include <string>
#include <optional>
#include <map>
#include <shared_mutex>
#include <memory>
#include <vector>
#include <cstdint>

class WAL;
class SSTable;

class HeliosDB {
public:
    explicit HeliosDB(const std::string& data_dir);
    ~HeliosDB();

    void put(const std::string& key, const std::string& value);
    std::optional<std::string> get(const std::string& key);
    void del(const std::string& key);

    void flush();
    void compact(); // size-tiered merge
    void close();

    void apply_put(const std::string& key, const std::string& value);
    void apply_delete(const std::string& key);

private:
    std::string data_directory_;
    std::string manifest_path_;
    uint64_t next_sst_id_{1};

    std::map<std::string, std::optional<std::string>> memtable_;
    size_t memtable_bytes_{0};
    static constexpr size_t kMaxMemtableBytes = 1 << 20;

    mutable std::shared_mutex mutex_;
    std::unique_ptr<WAL> wal_;
    std::vector<std::unique_ptr<SSTable>> sstables_; // newest first

    void load_manifest_and_sstables_();
    void write_manifest_atomic_(const std::vector<std::string>& files);
    std::vector<std::string> current_manifest_files_() const;

    std::string make_sstable_filename_(uint64_t id) const;

    void maybe_flush_unsafe_();
    void flush_unsafe_();

    static size_t kv_bytes_(const std::string& k, const std::optional<std::string>& v);
};