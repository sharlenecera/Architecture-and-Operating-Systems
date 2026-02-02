#include <algorithm>
#include <chrono>
#include <deque>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <optional>
#include <string>
#include <vector>
#include <thread>
#include <mutex>
#include <atomic>
using namespace std;

namespace fs = filesystem;


// -----------------------------
// Job model (students may extend)
// -----------------------------
struct Job {
    fs::path path;
    int arrival = 0; // "arrival time" in ticks (simple counter in this simulation)
    int est_cost = 1; // estimated cost (used for SJF/MLFQ ideas)
    int remaining = 1; // for RR/MLFQ: remaining "work units" (simple abstraction)
    int queue_level = 0; // for MLQ/MLFQ: which queue the job is in
};


// -----------------------------
// Small helpers
// -----------------------------
static string jsonEscape(const string& s) {
    // Minimal JSON string escape (good enough for file paths/names)
    string out;
    out.reserve(s.size() + 8);
    for (char c : s) {
        switch (c) {
            case '\\': out += "\\\\"; break;
            case '"':  out += "\\\""; break;
            case '\n': out += "\\n"; break;
            case '\r': out += "\\r"; break;
            case '\t': out += "\\t"; break;
            default:   out += c; break;
        }
    }
    return out;
}


static string permsToString(fs::perms p) {
    // POSIX-like rwx string, best-effort cross-platform
    auto bit = [&](fs::perms b) { return (p & b) != fs::perms::none; };

    string s;
    s += bit(fs::perms::owner_read)  ? 'r' : '-';
    s += bit(fs::perms::owner_write) ? 'w' : '-';
    s += bit(fs::perms::owner_exec)  ? 'x' : '-';

    s += bit(fs::perms::group_read)  ? 'r' : '-';
    s += bit(fs::perms::group_write) ? 'w' : '-';
    s += bit(fs::perms::group_exec)  ? 'x' : '-';

    s += bit(fs::perms::others_read)  ? 'r' : '-';
    s += bit(fs::perms::others_write) ? 'w' : '-';
    s += bit(fs::perms::others_exec)  ? 'x' : '-';
    return s;
}


// Convert filesystem::file_time_type to seconds since epoch (best-effort)
static long long fileTimeToEpochSeconds(fs::file_time_type ft) {
    // This conversion is a common approach for C++17; it’s “best-effort”
    using namespace chrono;
    auto sctp = time_point_cast<system_clock::duration>(
        ft - fs::file_time_type::clock::now() + system_clock::now()
    );
    return duration_cast<seconds>(sctp.time_since_epoch()).count();
}


// -----------------------------
// Indexing work (the "CPU burst")
// -----------------------------
static string scanOnePathJson(const fs::path& p) {
    // Build ONE JSON object (as a string) representing the file record.
    // Keep it simple: path, name, size, last_write_time, type flags, permissions, errors.
    string pathStr = p.string();
    string nameStr = p.filename().string();

    bool is_file = false;
    bool is_dir = false;
    bool is_symlink = false;
    unsigned long long size_bytes = 0;
    long long mtime_epoch = 0;
    string perms_rwx = "---------";
    string error;

    try {
        // status() follows symlinks; symlink_status() does not
        fs::file_status st = fs::symlink_status(p);

        is_symlink = fs::is_symlink(st);
        is_file = fs::is_regular_file(st);
        is_dir = fs::is_directory(st);

        // Size only valid for regular files
        if (is_file) {
            size_bytes = fs::file_size(p);
        }

        // Last write time
        mtime_epoch = fileTimeToEpochSeconds(fs::last_write_time(p));

        // Permissions
        perms_rwx = permsToString(st.permissions());
    }
    catch (const fs::filesystem_error& e) {
        error = e.what();
    }
    catch (const exception& e) {
        error = e.what();
    }

    // Note: Full ACLs on Windows (SIDs/ACEs) require platform APIs / extra libs.
    // This template records basic permission bits only.

    string json = "{";
    json += "\"path\":\"" + jsonEscape(pathStr) + "\",";
    json += "\"name\":\"" + jsonEscape(nameStr) + "\",";
    json += "\"is_file\":" + string(is_file ? "true" : "false") + ",";
    json += "\"is_dir\":" + string(is_dir ? "true" : "false") + ",";
    json += "\"is_symlink\":" + string(is_symlink ? "true" : "false") + ",";
    json += "\"size_bytes\":" + to_string(size_bytes) + ",";
    json += "\"mtime_epoch\":" + to_string(mtime_epoch) + ",";
    json += "\"perms_rwx\":\"" + perms_rwx + "\"";

    if (!error.empty()) {
        json += ",\"error\":\"" + jsonEscape(error) + "\"";
    }

    json += "}";
    return json;
}


// -----------------------------
// Job creation (workload)
// -----------------------------
static vector<Job> buildJobs(const fs::path& root) {
    vector<Job> jobs;
    int tick = 0;

    for (auto const& entry : fs::recursive_directory_iterator(root)) {
        if (!entry.is_regular_file()) continue; // keep it simple: index files only

        ++tick;
        fs::path p = entry.path();

        // Simple estimate: size buckets -> est_cost (1,2,3)
        unsigned long long size_bytes = 0;
        try {
            size_bytes = entry.file_size();
        } catch (...) {
            size_bytes = 0;
        }

        int est = 1;
        if (size_bytes >= 100000ULL && size_bytes < 10000000ULL) est = 2;   // 100KB..10MB
        else if (size_bytes >= 10000000ULL) est = 3;                        // >=10MB

        Job j;
        j.path = p;
        j.arrival = tick;
        j.est_cost = est;
        j.remaining = est;     // “work units” tied to cost
        j.queue_level = 0;     // starts high for MLFQ ideas
        jobs.push_back(j);
    }

    // Arrival order
    sort(jobs.begin(), jobs.end(), [](const Job& a, const Job& b) {
        return a.arrival < b.arrival;
    });

    return jobs;
}


// -----------------------------
// Scheduler hooks (students adapt)
// -----------------------------
static optional<Job> chooseNextJob(deque<Job>& ready, int /*tick*/) {
    /*
      STUDENT TASK: Replace this logic to implement a scheduler.

      Current behaviour: FCFS (pop from front)

      Ideas:
      - FCFS:         pop_front
      - SJF:          choose job with smallest est_cost (remove it)
      - Round Robin:  pop_front, run 1 unit, if remaining>0 push_back
      - MLQ:          multiple queues by queue_level, always choose highest queue first
      - MLFQ:         demote when it uses full quantum; boost occasionally
    */
    if (ready.empty()) return nullopt;

    Job j = ready.front();
    ready.pop_front();
    return j;
}


static void onJobFeedback(Job& /*job*/, const string& /*jsonRecord*/) {
    /*
      Optional STUDENT TASK:
      Use results to change scheduling behaviour (MLFQ-style).

      Examples:
      - If jsonRecord contains "error": demote job.queue_level
      - If job.est_cost is high: demote
      - If job finishes quickly: keep high priority
    */
}


// -----------------------------
// Simulation loop (runs "scheduler")
// -----------------------------
static void runIndexer(
    fs::path root,
    const fs::path& outputJsonl)
{
    auto start = chrono::high_resolution_clock::now();

    // set default directory if none
    if (root.empty()) root = fs::current_path();

    vector<Job> jobs = buildJobs(root);

    // In this simple model, all jobs are ready immediately.
    deque<Job> ready(jobs.begin(), jobs.end());

    ofstream out(outputJsonl);
    if (!out) {
        throw runtime_error("Could not open output file for writing.");
    }

    int tick = 0;

    while (!ready.empty()) {
        ++tick;

        auto next = chooseNextJob(ready, tick);
        if (!next.has_value()) continue;

        Job job = next.value();

        // "Run" job (index one file)
        string record = scanOnePathJson(job.path);

        // Add a few scheduling fields (simple: append before closing brace)
        // (Teaching-friendly; students can make proper JSON building later.)
        if (!record.empty() && record.back() == '}') {
            record.pop_back();
            record += ",\"arrival\":" + to_string(job.arrival);
            record += ",\"est_cost\":" + to_string(job.est_cost);
            record += ",\"queue_level\":" + to_string(job.queue_level);
            record += ",\"tick_ran\":" + to_string(tick);
            record += "}";
        }

        onJobFeedback(job, record);

        out << record << "\n";
    }

    auto end = chrono::high_resolution_clock::now();
    chrono::duration<double> elapsed = end - start;
    cout << "Control indexing for " << root
        << " took " << elapsed.count() << " seconds.\n";
}


static void run_indexer_threading(
    fs::path root,
    const fs::path& outputJsonl,
    unsigned int num_threads = thread::hardware_concurrency())
{
    auto start = chrono::high_resolution_clock::now();
    if (root.empty()) root = fs::current_path();

    vector<Job> jobs = buildJobs(root);

    // open shared output file
    ofstream out(outputJsonl);
    if (!out) {
        throw runtime_error("Cannot open output file for writing.");
    }

    atomic<size_t> next_index{0}; // which job index worker takes next
    atomic<int> tick{0}; // counter for when it ran, increases as a job finishes
    mutex out_mutex;

    // ensure there's at least 1 worker even if hardware_concurrency() returns 0
    if (num_threads == 0) num_threads = 1;

    // start threads
    vector<thread> threads;
    threads.reserve(num_threads);

    for (unsigned int w = 0; w < num_threads; ++w) {
        threads.emplace_back([&]{
            while (true) {
                // get next job index
                size_t i = next_index.fetch_add(1, memory_order_relaxed);
                if (i >= jobs.size()) break;

                const Job& job = jobs[i];
                string record = scanOnePathJson(job.path);

                int my_tick = tick.fetch_add(1, memory_order_relaxed) + 1;
                if (!record.empty() && record.back() == '}') {
                    record.pop_back();
                    record += ",\"arrival\":\" "     + to_string(job.arrival);
                    record += ",\"est_cost\":\" "    + to_string(job.est_cost);
                    record += ",\"queue_level\":\" " + to_string(job.queue_level);
                    record += ",\"tick_ran\":\" "    + to_string(my_tick);
                    record += "}";
                }

                onJobFeedback(const_cast<Job&>(job), record);

                {
                    lock_guard<mutex> lock(out_mutex);
                    out << record << "\n";
                }
            }
        });
    }
    for (auto& thread : threads) thread.join();

    auto end = chrono::high_resolution_clock::now();
    chrono::duration<double> elapsed = end - start;
    cout << "Threaded indexing for " << root
        << " took " << elapsed.count() << " seconds"
        << " using " << num_threads << " thread(s)\n";
}


int main() {
    try {
        runIndexer("", "cpp_index_results.jsonl");
        run_indexer_threading("", "cpp_index_results_threaded.jsonl", 4);
    }
    catch (const exception& e) {
        cerr << "Fatal error: " << e.what() << "\n";
        return 1;
    }
    return 0;
}
