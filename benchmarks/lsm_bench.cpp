#include "db.hpp"
#include <benchmark/benchmark.h>
#include <filesystem>

static void BM_WriteThroughput(benchmark::State& state) {
    std::filesystem::remove_all("bench_data");
    HeliosDB db("bench_data");

    for (auto _ : state) {
        for (int i = 0; i < 100000; i++) {
            db.put("key" + std::to_string(i), "value" + std::to_string(i));
        }
        db.flush();
    }

    state.SetItemsProcessed(state.iterations() * 100000);
}

static void BM_ReadThroughput(benchmark::State& state) {
    std::filesystem::remove_all("bench_data");
    HeliosDB db("bench_data");

    for (int i = 0; i < 200000; i++) {
        db.put("key" + std::to_string(i), "value" + std::to_string(i));
    }
    db.flush();

    for (auto _ : state) {
        for (int i = 0; i < 200000; i++) {
            benchmark::DoNotOptimize(db.get("key" + std::to_string(i)));
        }
    }

    state.SetItemsProcessed(state.iterations() * 200000);
}

BENCHMARK(BM_WriteThroughput);
BENCHMARK(BM_ReadThroughput);

BENCHMARK_MAIN();
