#pragma once

#include <string>
#include <optional>
#include <map>
#include <shared_mutex>
#include <memory>
#include <vector>
#include <cstdint>
#include <thread>
#include <condition_variable>
#include <atomic>

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
    void compact();
    void close();

    // Internal replay hooks (no WAL write)
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

    // Background compaction
    std::thread bg_;
    std::condition_variable cv_;
    std::mutex bg_mu_;
    std::atomic<bool> stop_{false};
    std::atomic<bool> compact_requested_{false};

    static constexpr size_t kCompactThreshold = 8;
    static constexpr size_t kMergeN = 4;

    void bg_loop_();
    void request_compaction_();

    void load_manifest_and_sstables_();
    void write_manifest_atomic_(const std::vector<std::string>& files);
    std::vector<std::string> read_manifest_files_() const;

    std::string make_sstable_filename_(uint64_t id) const;

    void maybe_flush_unsafe_();
    void flush_unsafe_();
    void compact_once_(); // performs one merge if possible

    static size_t kv_bytes_(const std::string& k, const std::optional<std::string>& v);
};
