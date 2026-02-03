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
#if defined(__APPLE__)
  #include <sys/types.h>
  #include <sys/wait.h>
  #include <unistd.h>
  #include <cerrno>
#endif
// below for hashing:
#include "digestpp/digestpp.hpp"
#include <sstream>

using namespace std;
using namespace digestpp;

namespace fs = filesystem;


static string hash_file(
    fs::path path,
    const string& hash_type = "sha256")
{
    if (path.empty()) path = fs::current_path();
    string algorithm;
    algorithm.reserve(hash_type.size());
    for (char c : hash_type) algorithm.push_back(static_cast<char>(tolower(static_cast<unsigned char>(c))));

    // aliases
    if (algorithm == "sha-256")  algorithm = "sha256";
    if (algorithm == "sha-512")  algorithm = "sha512";
    if (algorithm == "sha3")     algorithm = "sha3-256";
    if (algorithm == "blake2")   algorithm = "blake2b";

    // open file
    ifstream file(path, ios::binary);
    if (!file) {
        throw runtime_error("Could not open file for hashing: " + path.string());
    }

    // only including some algorithms
    if (algorithm == "sha256") {
        return sha256().absorb(file).hexdigest();
    } else if (algorithm == "sha512") {
        return sha512().absorb(file).hexdigest();
    } else if (algorithm == "sha3-256") {
        return sha3(256).absorb(file).hexdigest();
    } else if (algorithm == "sha3-512") {
        return sha3(512).absorb(file).hexdigest();
    } else if (algorithm == "blake2b" || algorithm == "blake2b512") {
        return blake2b(512).absorb(file).hexdigest();
    } else if (algorithm == "blake2s" || algorithm == "blake2s256") {
        return blake2s(256).absorb(file).hexdigest();
    } else if (algorithm == "md5") {
        return md5().absorb(file).hexdigest();
    }
    throw runtime_error("Unsupported/unknown hash algorithm: " + hash_type);
}


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
static string scanOnePathJson(
    const fs::path& p,
    const string hash_algorithm)
{
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
    string file_hash;

    try {
        // status() follows symlinks; symlink_status() does not
        fs::file_status st = fs::symlink_status(p);

        is_symlink = fs::is_symlink(st);
        is_file = fs::is_regular_file(st);
        is_dir = fs::is_directory(st);

        if (!hash_algorithm.empty() && is_file) {
            try {
                file_hash = hash_file(p, hash_algorithm);
            } catch (const exception& e) {
                file_hash = string("ERROR: ") + e.what();
            }
        }

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
    if (!file_hash.empty()) {
        json += ",\"hash_algorithm\":\"" + jsonEscape(hash_algorithm) + "\"";
        json += ",\"hash\":\"" + file_hash + "\"";
    }

    if (!error.empty()) {
        json += ",\"error\":\"" + jsonEscape(error) + "\"";
    }

    json += "}";
    return json;
}


// -----------------------------
// Job creation (workload)
// -----------------------------
static vector<Job> buildJobs(
    const fs::path& root,
    const int& greater_than)
{
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

        if (size_bytes < greater_than) continue;

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
static void run_indexer(
    fs::path root,
    const fs::path& outputJsonl,
    const string hash_algorithm = "sha256",
    const int& greater_than = 0)
{
    auto start = chrono::high_resolution_clock::now();

    // set default directory if none
    if (root.empty()) root = fs::current_path().parent_path();

    vector<Job> jobs = buildJobs(root, greater_than);

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
        string record = scanOnePathJson(job.path, hash_algorithm);

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
    cout << "Control indexing\n" << "Root: " << root << "\n"
        << "Hash algorithm: " << hash_algorithm << "\n"
        << "Greater than (bytes): " << (greater_than ? to_string(greater_than) : "") << (greater_than ? "\n" : "")    
        << "Elapsed time: " << elapsed.count() << " seconds \n"
        << "---------------------------------------------------------------------------\n";
}


static void run_indexer_threading(
    fs::path root,
    const fs::path& outputJsonl,
    const string hash_algorithm = "sha256",
    const int& greater_than = 0,
    unsigned int num_threads = thread::hardware_concurrency())
{
    auto start = chrono::high_resolution_clock::now();
    if (root.empty()) root = fs::current_path().parent_path();

    vector<Job> jobs = buildJobs(root, greater_than);

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
                string record = scanOnePathJson(job.path, hash_algorithm);

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
    cout << "Threaded indexing\n" << "Root: " << root << "\n"
        << "Hash algorithm: " << hash_algorithm << "\n"
        << "Greater than (bytes): " << (greater_than ? to_string(greater_than) : "") << (greater_than ? "\n" : "")
        << "Elapsed time: " << elapsed.count() << " seconds\n"
        << "Number of threads: " << num_threads << "\n"
        << "---------------------------------------------------------------------------\n";
}


static void run_indexer_multiprocessing(
    fs::path root,
    const fs::path& outputJsonl,
    const string hash_algorithm = "sha256",
    const int& greater_than = 0,
    unsigned int num_processes = thread::hardware_concurrency())
{
#if defined(__APPLE__)
    auto start_time = chrono::high_resolution_clock::now();
    if (root.empty()) root = fs::current_path().parent_path();
    vector<Job> jobs = buildJobs(root, greater_than);

    // if nothing to do, create and return the output
    {
        ofstream out(outputJsonl);
        if (!out) throw runtime_error("Cannot open output file for writing.");
        if (jobs.empty()) {
            cout << "Done (multiprocess). Wrote empty file: " << outputJsonl << "\n";
            return;
        }
    }

    // ensure at least 1 process
    if (num_processes == 0) num_processes = 1;
    if (num_processes > jobs.size()) num_processes = static_cast<unsigned int>(jobs.size());


    // compute close-to-equal slices for each worker
    const size_t N = jobs.size();
    const size_t base = N / num_processes;
    const size_t remainder  = N % num_processes;

    vector<pair<size_t,size_t>> ranges;
    ranges.reserve(num_processes);

    size_t start = 0;
    for (unsigned int i = 0; i < num_processes; ++i) {
        size_t len = base + (i < remainder ? 1 : 0); // split remainder into slices
        size_t end = start + len;
        ranges.emplace_back(start, end);
        start = end;
    }

    // put temp files in same directory as output file
    fs::path out_dir = outputJsonl.has_parent_path() ? outputJsonl.parent_path() : fs::current_path();
    const string base_name = outputJsonl.filename().string();

    vector<pid_t> pids;
    vector<fs::path> part_files;
    pids.reserve(num_processes);
    part_files.reserve(num_processes);

    // fork worker processes
    for (unsigned int i = 0; i < num_processes; ++i) {
        // make a temp file
        fs::path part = out_dir / (base_name + ".part." + to_string(i) + ".tmp");

        pid_t pid = ::fork();
        if (pid == 0) { // ------ CHILD process ------
            // open part file
            ofstream part_out(part);
            if (!part_out) {
                // child: exit with to allow proper code clean up of forked process
                _exit(111);
            }

            int local_tick = 0;
            const auto [s, e] = ranges[i];
            for (size_t j = s; j < e; ++j) {
                Job job = jobs[j];
                string record = scanOnePathJson(job.path, hash_algorithm);

                ++local_tick;
                if (!record.empty() && record.back() == '}') {
                    record.pop_back();
                    record += ",\"arrival\":\" "     + to_string(job.arrival);
                    record += ",\"est_cost\":\" "    + to_string(job.est_cost);
                    record += ",\"queue_level\":\" " + to_string(job.queue_level);
                    record += ",\"tick_ran\":\" "    + to_string(local_tick);
                    record += ",\"proc_index\":\" "  + to_string(i);
                    record += "}";
                }

                onJobFeedback(job, record);
                part_out << record << "\n";
            }

            part_out.close();
            _exit(0);   // means success
        }
        else if (pid > 0) { // ------ PARENT process ------
            pids.push_back(pid);
            part_files.push_back(part);
        }
        else { // ------ fork failed ------
            throw runtime_error("fork() failed: " + to_string(errno));
        }
    }

    // wait for all children to finish
    for (pid_t pid : pids) {
        int status = 0;
        if (::waitpid(pid, &status, 0) == -1) {
            throw runtime_error("waitpid() failed: " + to_string(errno));
        }
        // check if child terminated normally or exit successfully
        if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
            throw runtime_error("Child process failed (pid " + to_string(pid) + ")");
        }
    }

    // concatenate part files in slice order to make final output
    {
        ofstream out(outputJsonl, ios::trunc);
        if (!out) throw runtime_error("Could not open output file for concatenation.");

        for (unsigned int i = 0; i < num_processes; ++i) {
            ifstream in(part_files[i], ios::binary);
            if (!in) throw runtime_error("Could not open part file: " + part_files[i].string());
            out << in.rdbuf();
        }
    }

    // delete each .tmp file
    for (const auto& p : part_files) {
        error_code ec;
        fs::remove(p, ec);
    }


    auto end_time = chrono::high_resolution_clock::now();
    chrono::duration<double> elapsed = end_time - start_time;
    cout << "Multiprocessed indexing\n" << "Directory: " << root << "\n"
        << "Hash algorithm: " << hash_algorithm << "\n"
        << "Greater than (bytes): " << (greater_than ? to_string(greater_than) : "") << (greater_than ? "\n" : "")
        << "Elapsed time: " << elapsed.count() << " seconds\n"
        << "Number of processes: " << num_processes << "\n"
        << "---------------------------------------------------------------------------\n";

#else
    (void)root; (void)outputJsonl; (void)num_processes;
    throw runtime_error("run_indexer_multiprocessing is POSIX-only (macOS/Linux).");
#endif
}


int main(int argc, char** argv) {
    fs::path root = "";
    int greater_than = 0;
    int threads = thread::hardware_concurrency();
    int processes = thread::hardware_concurrency();
    fs::path lin_out_file = "cpp_index_results.jsonl";
    fs::path th_out_file = "cpp_index_results_threaded.jsonl";
    fs::path mp_out_file = "cpp_index_results_multiprocessed.jsonl";
    string hash_algorithm = "sha256";
    fs::path file_to_hash = "";
    
    for (int i = 1; i < argc; ++i) {
        string arg = argv[i];

        if (arg == "--root" && i + 1 < argc) {
            root = argv[++i];
        }
        else if (arg == "--linear_output" && i + 1 < argc) {
            lin_out_file = argv[++i];
        }
        else if (arg == "--threading_output" && i + 1 < argc) {
            th_out_file = argv[++i];
        }
        else if (arg == "--multiprocessing_output" && i + 1 < argc) {
            mp_out_file = argv[++i];
        }
        else if (arg == "--greater_than" && i + 1 < argc) {
            greater_than = stoll(argv[++i]);
        }
        else if (arg == "--threads" && i + 1 < argc) {
            threads = stoll(argv[++i]);
        }
        else if (arg == "--processes" && i + 1 < argc) {
            processes = stoll(argv[++i]);
        }
        else if (arg == "--hash" && i + 1 < argc) {
            hash_algorithm = argv[++i];
        }
        else if (arg == "--file_to_hash" && i + 1 < argc) {
            file_to_hash = argv[++i];
        }
        else if (arg == "--help") {
            cout << "Usage:\n"
                        << "  --root <directory>\n"
                        << "  --linear_output <filename>\n"
                        << "  --threading_output <filename>\n"
                        << "  --multiprocessing_output <filename>\n"
                        << "  --greater_than <bytes>\n"
                        << "  --threads <num_threads>\n"
                        << "  --processes <num_processes>\n"
                        << "  --hash <algorithm>      "
                        << "(default:sha256|sha512|sha3-256|sha3-512|blake2b|blake2s|md5)\n"
                        << "  --file_to_hash <filename> (indexer does not run with this)\n";
            return 0;
        }
        else {
            cerr << "Unknown flag: " << arg << "\n";
            return 1;
        }
    }

    try {
        
        // hash file if specified
        if (!file_to_hash.empty()){
            string hex_hash = hash_file(file_to_hash, hash_algorithm);
            cout << "Hash\n" << "File: " << (file_to_hash.empty() ? "." : file_to_hash) << "\n"
                << "Algorithm: " << hash_algorithm << "\n"
                << "Result: " << hex_hash << "\n"
                << "---------------------------------------------------------------------------\n";
        }
        // otherwise run indexer
        else {
            run_indexer(root, lin_out_file, hash_algorithm, greater_than);
            run_indexer_threading(root, th_out_file, hash_algorithm, greater_than, threads);
            run_indexer_multiprocessing(root, mp_out_file, hash_algorithm, greater_than, processes);
        }
    }
    catch (const exception& e) {
        cerr << "Fatal error: " << e.what() << "\n";
        return 1;
    }
    return 0;
}
