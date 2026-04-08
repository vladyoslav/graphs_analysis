#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include <cxxopts.hpp>
#include <spla.hpp>

#include "lib.hpp"

namespace fs = std::filesystem;

namespace {

    // RFC-style CSV field quoting only when needed (comma, quote, newline).
    std::string csv_field(std::string_view s) {
        if (s.find_first_of(",\"\r\n") == std::string_view::npos) {
            return std::string{s};
        }
        std::string out = "\"";
        out.reserve(s.size() + 2);
        for (char c : s) {
            if (c == '"') {
                out += "\"\"";
            } else {
                out += c;
            }
        }
        out += '"';
        return out;
    }

    std::uint64_t mst_total_weight(const std::vector<MstEdge>& mst) {
        std::uint64_t s = 0;
        for (const MstEdge& e : mst) {
            s += e.w;
        }
        return s;
    }

    std::size_t directed_edge_count(const spla::ref_ptr<spla::Matrix>& A) {
        spla::ref_ptr<spla::MemView> rows_view;
        spla::ref_ptr<spla::MemView> cols_view;
        spla::ref_ptr<spla::MemView> vals_view;
        if (A->read(rows_view, cols_view, vals_view) != spla::Status::Ok) {
            return 0;
        }
        return vals_view->get_size() / sizeof(std::uint32_t);
    }

    void write_result_csv(const fs::path&            out_path,
                          const std::string&         graph_label,
                          spla::uint                 n,
                          std::size_t                directed_edges,
                          std::uint64_t              mst_weight,
                          std::size_t                mst_edges,
                          const std::vector<double>& times_ms) {
        std::ofstream out(out_path);
        if (!out) {
            throw std::runtime_error("cannot open output file: " + out_path.string());
        }
        out << std::fixed << std::setprecision(6);
        out << "library,graph,vertices,edges,cores,mst_weight,mst_edges,time_ms\n";
        for (double time_ms : times_ms) {
            out << "spla," << csv_field(graph_label) << ',' << n << ',' << directed_edges << ",1,"
                << mst_weight << ',' << mst_edges << ',' << time_ms << '\n';
        }
    }

}// namespace

auto main(int argc, char* argv[]) -> int {
    cxxopts::Options cli("boruvka_spla", "Borůvka MST benchmark (SPLA)");

    // clang-format off
    cli.add_options()
        ("m,mtxpath", "Path to Matrix Market .mtx graph", cxxopts::value<std::string>())
        ("o,out",     "Output CSV path (benchmark table)", cxxopts::value<std::string>())
        ("n,niters",  "Number of timed benchmark runs", cxxopts::value<int>()->default_value("10"))
        ("w,warmup",  "Warmup runs (not timed)", cxxopts::value<int>()->default_value("3"))
        ("c,cpu-only","set_accelerator(None), set_force_no_acceleration(true), CpuCsr", cxxopts::value<bool>()->implicit_value("true")->default_value("false"))
        ("p,platform","SPLA platform index (ignored with --cpu-only)", cxxopts::value<int>()->default_value("0"))
        ("d,device",  "SPLA device index (ignored with --cpu-only)", cxxopts::value<int>()->default_value("0"))
        ("h,help",    "Print help");
    // clang-format on

    cxxopts::ParseResult parsed;
    try {
        parsed = cli.parse(argc, argv);
    } catch (const cxxopts::exceptions::exception& e) {
        std::cerr << e.what() << "\n";
        std::cerr << cli.help() << std::endl;
        return 1;
    }

    if (parsed.count("help") != 0) {
        std::cout << cli.help() << std::endl;
        return 0;
    }

    if (parsed.count("mtxpath") == 0 || parsed.count("out") == 0) {
        std::cerr << "required: --mtxpath and --out\n\n";
        std::cerr << cli.help() << std::endl;
        return 1;
    }

    const std::string mtxpath  = parsed["mtxpath"].as<std::string>();
    const fs::path    out_path = parsed["out"].as<std::string>();
    const int         niters   = parsed["niters"].as<int>();
    const int         warmup   = parsed["warmup"].as<int>();
    const int         platform = parsed["platform"].as<int>();
    const int         device   = parsed["device"].as<int>();
    const bool        cpu_only = parsed["cpu-only"].as<bool>();

    if (niters < 1) {
        std::cerr << "niters must be >= 1\n";
        return 1;
    }
    if (warmup < 0) {
        std::cerr << "warmup must be >= 0\n";
        return 1;
    }

    std::string              acc_info;
    spla::Library*           library = spla::Library::get();
    const spla::FormatMatrix graph_format =
            cpu_only ? spla::FormatMatrix::CpuCsr : spla::FormatMatrix::AccCsr;

    if (cpu_only) {
        if (library->set_accelerator(spla::AcceleratorType::None) != spla::Status::Ok) {
            std::cerr << "set_accelerator(None) failed\n";
            return 1;
        }
        if (library->set_force_no_acceleration(true) != spla::Status::Ok) {
            std::cerr << "set_force_no_acceleration(true) failed\n";
            return 1;
        }
    } else {
        if (library->set_force_no_acceleration(false) != spla::Status::Ok) {
            std::cerr << "set_force_no_acceleration(false) failed\n";
            return 1;
        }
        (void) library->set_platform(platform);
        (void) library->set_device(device);
        (void) library->set_queues_count(1);
    }
    library->get_accelerator_info(acc_info);
    std::cout << "env: " << acc_info;
    if (cpu_only) {
        std::cout << " (accelerator disabled, force_no_acceleration, CpuCsr)";
    }
    std::cout << std::endl;

    try {
        spla::ref_ptr<spla::Matrix> graph = load_graph(mtxpath, graph_format);
        if (!graph) {
            std::cerr << "load_graph returned null\n";
            library->finalize();
            return 1;
        }

        const spla::uint  n              = graph->get_n_rows();
        const std::size_t directed_edges = directed_edge_count(graph);

        std::vector<MstEdge> ref_mst = boruvka_mst(graph, graph_format);
        const std::uint64_t  w_sum   = mst_total_weight(ref_mst);
        const std::size_t    m_edges = ref_mst.size();

        for (int i = 0; i < warmup; ++i) {
            (void) boruvka_mst(graph, graph_format);
        }

        std::vector<double> times_ms;
        times_ms.reserve(static_cast<std::size_t>(niters));

        spla::Timer timer;
        for (int i = 0; i < niters; ++i) {
            timer.start();
            (void) boruvka_mst(graph, graph_format);
            timer.stop();
            times_ms.push_back(timer.get_elapsed_ms());
        }

        const std::string graph_label = fs::path(mtxpath).filename().string();

        write_result_csv(out_path, graph_label, n, directed_edges, w_sum, m_edges, times_ms);

        library->finalize();
    } catch (const std::exception& e) {
        std::cerr << e.what() << '\n';
        spla::Library::get()->finalize();
        return 1;
    }

    return 0;
}
