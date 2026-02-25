#include "db.hpp"
#include "wal.hpp"
#include "sstable.hpp"

#include <filesystem>
#include <fstream>
#include <sstream>
#include <algorithm>
#include <iomanip>
#include <limits>

using namespace std;

static bool starts_with(const std::string& s, const std::string& p) {
    return s.size() >= p.size() && std::equal(p.begin(), p.end(), s.begin());
}

HeliosDB::HeliosDB(const std::string& data_dir)
    : data_directory_(data_dir),
      manifest_path_(data_dir + "/manifest.txt")
{
    std::filesystem::create_directories(data_directory_);
    load_manifest_and_sstables_();

    wal_ = std::make_unique<WAL>(data_directory_ + "/wal.log");
    wal_->replay(*this);

    // background compaction thread
    bg_ = std::thread([this] { bg_loop_(); });
}

HeliosDB::~HeliosDB() {
    close();
}

void HeliosDB::close() {
    // stop background thread
    stop_.store(true);
    {
        std::lock_guard<std::mutex> lk(bg_mu_);
        compact_requested_.store(true);
    }
    cv_.notify_all();
    if (bg_.joinable()) bg_.join();
}

size_t HeliosDB::kv_bytes_(const std::string& k, const std::optional<std::string>& v) {
    return k.size() + (v ? v->size() : 0) + 16;
}

void HeliosDB::apply_put(const std::string& key, const std::string& value) {
    std::unique_lock lock(mutex_);
    auto it = memtable_.find(key);
    if (it != memtable_.end()) memtable_bytes_ -= kv_bytes_(key, it->second);
    memtable_[key] = value;
    memtable_bytes_ += kv_bytes_(key, memtable_[key]);
}

void HeliosDB::apply_delete(const std::string& key) {
    std::unique_lock lock(mutex_);
    auto it = memtable_.find(key);
    if (it != memtable_.end()) memtable_bytes_ -= kv_bytes_(key, it->second);
    memtable_[key] = std::nullopt;
    memtable_bytes_ += kv_bytes_(key, memtable_[key]);
}

void HeliosDB::put(const std::string& key, const std::string& value) {
    std::unique_lock lock(mutex_);
    wal_->append_put(key, value);

    auto it = memtable_.find(key);
    if (it != memtable_.end()) memtable_bytes_ -= kv_bytes_(key, it->second);
    memtable_[key] = value;
    memtable_bytes_ += kv_bytes_(key, memtable_[key]);

    maybe_flush_unsafe_();
}

void HeliosDB::del(const std::string& key) {
    std::unique_lock lock(mutex_);
    wal_->append_delete(key);

    auto it = memtable_.find(key);
    if (it != memtable_.end()) memtable_bytes_ -= kv_bytes_(key, it->second);
    memtable_[key] = std::nullopt;
    memtable_bytes_ += kv_bytes_(key, memtable_[key]);

    maybe_flush_unsafe_();
}

std::optional<std::string> HeliosDB::get(const std::string& key) {
    {
        std::shared_lock lock(mutex_);
        auto it = memtable_.find(key);
        if (it != memtable_.end()) {
            if (!it->second.has_value()) return std::nullopt;
            return it->second.value();
        }
    }

    // newest -> oldest
    for (const auto& sst : sstables_) {
        auto v = sst->get(key);
        if (v.has_value()) {
            const auto& inner = v.value();
            if (!inner.has_value()) return std::nullopt;
            return inner.value();
        }
    }
    return std::nullopt;
}

void HeliosDB::maybe_flush_unsafe_() {
    if (memtable_bytes_ >= kMaxMemtableBytes) {
        flush_unsafe_();
    }
}

std::string HeliosDB::make_sstable_filename_(uint64_t id) const {
    std::ostringstream oss;
    oss << "sst_" << std::setw(6) << std::setfill('0') << id << ".dat";
    return oss.str();
}

std::vector<std::string> HeliosDB::read_manifest_files_() const {
    std::vector<std::string> files;
    std::ifstream in(manifest_path_);
    std::string line;
    while (std::getline(in, line)) {
        if (!line.empty()) files.push_back(line);
    }
    return files;
}

void HeliosDB::write_manifest_atomic_(const std::vector<std::string>& files) {
    const std::string tmp = manifest_path_ + ".tmp";
    {
        std::ofstream out(tmp, std::ios::trunc);
        for (const auto& f : files) out << f << "\n";
        out.flush();
    }
    std::filesystem::rename(tmp, manifest_path_);
}

void HeliosDB::load_manifest_and_sstables_() {
    if (!std::filesystem::exists(manifest_path_)) {
        std::ofstream(manifest_path_).close();
        next_sst_id_ = 1;
        return;
    }

    auto files = read_manifest_files_();

    for (const auto& f : files) {
        if (starts_with(f, "sst_") && f.size() >= 10) {
            std::string num = f.substr(4, 6);
            uint64_t id = 0;
            try { id = std::stoull(num); } catch (...) { id = 0; }
            next_sst_id_ = std::max(next_sst_id_, id + 1);
        }
    }

    std::vector<std::unique_ptr<SSTable>> loaded;
    for (const auto& f : files) {
        std::string path = data_directory_ + "/" + f;
        if (std::filesystem::exists(path) && SSTable::is_valid(path)) {
            loaded.push_back(std::make_unique<SSTable>(path));
        }
    }
    std::reverse(loaded.begin(), loaded.end());
    sstables_ = std::move(loaded);

    // clean manifest
    std::vector<std::string> cleaned;
    for (const auto& f : files) {
        std::string path = data_directory_ + "/" + f;
        if (std::filesystem::exists(path) && SSTable::is_valid(path)) cleaned.push_back(f);
    }
    if (cleaned != files) write_manifest_atomic_(cleaned);
}

void HeliosDB::request_compaction_() {
    {
        std::lock_guard<std::mutex> lk(bg_mu_);
        compact_requested_.store(true);
    }
    cv_.notify_one();
}

void HeliosDB::flush_unsafe_() {
    if (memtable_.empty()) return;

    const uint64_t id = next_sst_id_++;
    const std::string filename = make_sstable_filename_(id);
    const std::string path = data_directory_ + "/" + filename;

    std::vector<std::pair<std::string, std::optional<std::string>>> entries;
    entries.reserve(memtable_.size());
    for (const auto& [k, v] : memtable_) entries.push_back({k, v});

    SSTable::write_atomic(path, entries);

    auto files = read_manifest_files_();
    files.push_back(filename);
    write_manifest_atomic_(files);

    sstables_.insert(sstables_.begin(), std::make_unique<SSTable>(path));

    memtable_.clear();
    memtable_bytes_ = 0;
    wal_->reset();

    if (sstables_.size() >= kCompactThreshold) request_compaction_();
}

void HeliosDB::flush() {
    std::unique_lock lock(mutex_);
    flush_unsafe_();
}

void HeliosDB::compact() {
    request_compaction_();
}

void HeliosDB::bg_loop_() {
    std::unique_lock<std::mutex> lk(bg_mu_);
    while (!stop_.load()) {
        cv_.wait(lk, [&] { return stop_.load() || compact_requested_.load(); });
        if (stop_.load()) break;
        compact_requested_.store(false);

        lk.unlock();
        // do one merge at a time
        compact_once_();
        lk.lock();
    }
}

void HeliosDB::compact_once_() {
    // Snapshot manifest files under DB lock
    std::vector<std::string> files;
    {
        std::unique_lock lock(mutex_);
        files = read_manifest_files_();
        if (files.size() < kMergeN) return;
    }

    // Merge newest kMergeN files: last kMergeN in manifest (manifest oldest->newest)
    std::vector<std::string> merge_files(files.end() - kMergeN, files.end());

    // Build merged map (oldest->newest so newest wins)
    std::map<std::string, std::optional<std::string>> merged;
    const uint32_t TOMBSTONE = std::numeric_limits<uint32_t>::max();

    for (const auto& f : merge_files) {
        std::string p = data_directory_ + "/" + f;
        if (!std::filesystem::exists(p) || !SSTable::is_valid(p)) continue;

        std::ifstream in(p, std::ios::binary);
        if (!in.is_open()) continue;

        auto total = std::filesystem::file_size(p);
        uint64_t end = static_cast<uint64_t>(total - sizeof(uint64_t) - sizeof(uint32_t)); // footer

        uint64_t off = 0;
        while (off < end) {
            in.seekg(static_cast<std::streamoff>(off), std::ios::beg);
            uint32_t ksize=0, vsize=0;
            in.read(reinterpret_cast<char*>(&ksize), 4);
            if (!in) break;
            in.read(reinterpret_cast<char*>(&vsize), 4);
            if (!in) break;
            if (off + 8ULL + ksize > end) break;

            std::string key(ksize, '\0');
            in.read(key.data(), ksize);
            if (!in) break;

            if (vsize == TOMBSTONE) {
                merged[key] = std::nullopt;
                off = static_cast<uint64_t>(in.tellg());
            } else {
                if (off + 8ULL + ksize + vsize > end) break;
                std::string val(vsize, '\0');
                in.read(val.data(), vsize);
                if (!in) break;
                merged[key] = std::move(val);
                off = static_cast<uint64_t>(in.tellg());
            }
        }
    }

    // Write new merged SSTable
    uint64_t new_id = 0;
    std::string out_file;
    std::string out_path;
    {
        std::unique_lock lock(mutex_);
        new_id = next_sst_id_++;
        out_file = make_sstable_filename_(new_id);
        out_path = data_directory_ + "/" + out_file;
    }

    std::vector<std::pair<std::string, std::optional<std::string>>> entries;
    entries.reserve(merged.size());
    for (auto& [k, v] : merged) entries.push_back({k, v});

    SSTable::write_atomic(out_path, entries);

    // Install: rewrite manifest + delete old files + reload sstables (under lock)
    {
        std::unique_lock lock(mutex_);

        auto cur = read_manifest_files_();
        if (cur.size() < kMergeN) return;

        std::vector<std::string> new_manifest(cur.begin(), cur.end() - kMergeN);
        new_manifest.push_back(out_file);
        write_manifest_atomic_(new_manifest);

        for (const auto& f : merge_files) {
            std::filesystem::remove(data_directory_ + "/" + f);
            std::filesystem::remove(data_directory_ + "/" + f + ".bloom");
        }

        load_manifest_and_sstables_();
    }
}
