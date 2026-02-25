#include "db.hpp"
#include "wal.hpp"
#include "sstable.hpp"

#include <filesystem>
#include <fstream>
#include <sstream>
#include <algorithm>
#include <iomanip>
#include <unordered_map>

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
}

HeliosDB::~HeliosDB() { close(); }

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
    if (memtable_bytes_ >= kMaxMemtableBytes) flush_unsafe_();
}

std::string HeliosDB::make_sstable_filename_(uint64_t id) const {
    std::ostringstream oss;
    oss << "sst_" << std::setw(6) << std::setfill('0') << id << ".dat";
    return oss.str();
}

std::vector<std::string> HeliosDB::current_manifest_files_() const {
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

    auto files = current_manifest_files_();

    // compute next id
    for (const auto& f : files) {
        if (starts_with(f, "sst_") && f.size() >= 10) {
            std::string num = f.substr(4, 6);
            uint64_t id = 0;
            try { id = std::stoull(num); } catch (...) { id = 0; }
            next_sst_id_ = std::max(next_sst_id_, id + 1);
        }
    }

    // load valid tables only, newest first
    std::vector<std::unique_ptr<SSTable>> loaded;
    for (const auto& f : files) {
        std::string path = data_directory_ + "/" + f;
        if (std::filesystem::exists(path) && SSTable::is_valid(path)) {
            loaded.push_back(std::make_unique<SSTable>(path));
        }
    }
    std::reverse(loaded.begin(), loaded.end());
    sstables_ = std::move(loaded);

    // rewrite manifest removing missing/corrupt entries
    std::vector<std::string> cleaned;
    for (const auto& f : files) {
        std::string path = data_directory_ + "/" + f;
        if (std::filesystem::exists(path) && SSTable::is_valid(path)) cleaned.push_back(f);
    }
    if (cleaned != files) write_manifest_atomic_(cleaned);
}

void HeliosDB::flush_unsafe_() {
    if (memtable_.empty()) return;

    const uint64_t id = next_sst_id_++;
    const std::string filename = make_sstable_filename_(id);
    const std::string path = data_directory_ + "/" + filename;

    std::vector<std::pair<std::string, std::optional<std::string>>> entries;
    entries.reserve(memtable_.size());
    for (const auto& [k, v] : memtable_) entries.push_back({k, v});

    // atomic write + validate
    SSTable::write_atomic(path, entries);

    // update manifest atomically
    auto files = current_manifest_files_();
    files.push_back(filename);
    write_manifest_atomic_(files);

    // add table newest-first
    sstables_.insert(sstables_.begin(), std::make_unique<SSTable>(path));

    // clear memtable + reset WAL (data now durable in SSTable)
    memtable_.clear();
    memtable_bytes_ = 0;
    wal_->reset();

    // optional: compact if too many tables
    if (sstables_.size() >= 8) compact();
}

void HeliosDB::flush() {
    std::unique_lock lock(mutex_);
    flush_unsafe_();
}

void HeliosDB::compact() {
    // size-tiered: merge newest 4 tables into 1
    if (sstables_.size() < 4) return;

    const size_t kMergeN = 4;

    // Load manifest list
    auto files = current_manifest_files_();
    // files are oldest->newest in manifest; but our sstables_ is newest->oldest.
    // We will merge the newest kMergeN files: last kMergeN in manifest.
    if (files.size() < kMergeN) return;

    std::vector<std::string> merge_files(files.end() - kMergeN, files.end());

    // Multi-way merge using map (lean and correct)
    std::map<std::string, std::optional<std::string>> merged; // newest wins by applying in order
    // Apply from oldest->newest among the selected tables to make newest win
    for (const auto& f : merge_files) {
        std::string p = data_directory_ + "/" + f;
        SSTable t(p);

        // brute scan: since we don't have iterator yet, we reconstruct by reading file sequentially
        // lean approach: re-open as raw parse using SSTable::get isn't enough for full scan.
        // So: we re-parse the file here.
        std::ifstream in(p, std::ios::binary);
        if (!in.is_open()) continue;

        auto total = std::filesystem::file_size(p);
        uint64_t end = static_cast<uint64_t>(total - sizeof(uint64_t) - sizeof(uint32_t)); // footer size (magic+checksum)
        uint64_t off = 0;

        while (off < end) {
            in.seekg(static_cast<std::streamoff>(off), std::ios::beg);
            uint32_t ksize = 0, vsize = 0;
            in.read(reinterpret_cast<char*>(&ksize), 4);
            if (!in) break;
            in.read(reinterpret_cast<char*>(&vsize), 4);
            if (!in) break;
            if (off + 8ULL + ksize > end) break;

            std::string key(ksize, '\0');
            in.read(key.data(), ksize);
            if (!in) break;

            if (vsize == std::numeric_limits<uint32_t>::max()) {
                merged[key] = std::nullopt;
                off = static_cast<uint64_t>(in.tellg());
            } else {
                if (off + 8ULL + ksize + vsize > end) break;
                std::string val(vsize, '\0');
                in.read(val.data(), vsize);
                if (!in) break;
                merged[key] = val;
                off = static_cast<uint64_t>(in.tellg());
            }
        }
    }

    const uint64_t id = next_sst_id_++;
    const std::string out_file = make_sstable_filename_(id);
    const std::string out_path = data_directory_ + "/" + out_file;

    std::vector<std::pair<std::string, std::optional<std::string>>> entries;
    entries.reserve(merged.size());
    for (auto& [k, v] : merged) entries.push_back({k, v});

    SSTable::write_atomic(out_path, entries);

    // Remove merged files from manifest, append new one
    std::vector<std::string> new_manifest(files.begin(), files.end() - kMergeN);
    new_manifest.push_back(out_file);
    write_manifest_atomic_(new_manifest);

    // Delete old files on disk
    for (const auto& f : merge_files) {
        std::filesystem::remove(data_directory_ + "/" + f);
    }

    // Reload sstables from manifest (simple and safe)
    load_manifest_and_sstables_();
}

void HeliosDB::close() {}