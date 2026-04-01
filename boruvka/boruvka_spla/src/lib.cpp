#include "lib.hpp"

#include <cstdint>
#include <fstream>
#include <numeric>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace {

    // DSU for boruvka_mst: representative = min vertex id in the component (not union-by-rank like DsuCompact in boruvka_classic.cpp).
    struct Dsu {
        std::vector<spla::uint> parent;

        explicit Dsu(spla::uint n) : parent(n) {
            std::iota(parent.begin(), parent.end(), spla::uint{0});
        }

        bool is_representative(spla::uint i) const { return parent[i] == i; }

        spla::uint find(spla::uint x) {
            spla::uint r = x;
            while (parent[r] != r) {
                r = parent[r];
            }
            while (parent[x] != x) {
                const spla::uint p = parent[x];
                parent[x]          = r;
                x                  = p;
            }
            return r;
        }

        void unite(spla::uint a, spla::uint b) {
            a = find(a);
            b = find(b);
            if (a == b) {
                return;
            }
            if (a < b) {
                parent[b] = a;
            } else {
                parent[a] = b;
            }
        }
    };

    // Skip empty lines and Matrix Market comments (% ...). Return the first other line.
    std::string read_first_data_line(std::istream& in) {
        std::string line;
        while (std::getline(in, line)) {
            if (!line.empty() && line[0] != '%') {
                return line;
            }
        }
        throw std::runtime_error("mtx: end of file before size line (expected n m nnz)");
    }

    void parse_size_line(const std::string& line, spla::uint& n, std::size_t& nnz) {
        spla::uint         m = 0;
        std::size_t        z = 0;
        std::istringstream iss(line);
        if (!(iss >> n >> m >> z)) {
            throw std::runtime_error("mtx: size line must have three integers (n m nnz): " + line);
        }
        if (n == 0 || m == 0) {
            throw std::runtime_error("mtx: n and m must be positive");
        }
        if (n != m) {
            throw std::runtime_error("mtx: need a square matrix (n == m)");
        }
        nnz = z;
    }

}// namespace

// --- Matrix Market loader: undirected UINT adjacency with packed (weight, neighbor) per entry. ---

spla::ref_ptr<spla::Matrix> load_graph(const std::string& mtx_path) {
    std::ifstream in(mtx_path);
    if (!in.is_open()) {
        throw std::runtime_error("mtx: cannot open " + mtx_path);
    }

    spla::uint  n   = 0;
    std::size_t nnz = 0;
    // Phase: parse dimensions (n, nnz) from the first non-comment line.
    parse_size_line(read_first_data_line(in), n, nnz);

    if (n > EDGE_ENCODE_MAX_N) {
        throw std::runtime_error("mtx: n must be <= 2^EDGE_ENCODE_SHIFT (" +
                                 std::to_string(EDGE_ENCODE_MAX_N) + ")");
    }

    // Phase: allocate n×n UINT matrix; absent edges use INF (must not collide with encode_edge).
    spla::ref_ptr<spla::Matrix> A = spla::Matrix::make(n, n, spla::UINT);
    if (!A) {
        throw std::runtime_error("mtx: failed to allocate adjacency matrix");
    }
    A->set_format(spla::FormatMatrix::AccCsr);
    if (A->set_fill_value(spla::Scalar::make_uint(INF)) != spla::Status::Ok) {
        throw std::runtime_error("mtx: set_fill_value failed");
    }

    // Phase: read nnz lines "u v [w]" (1-based); missing w defaults to 1; store undirected encoded edges (u,v) and (v,u).
    std::string line;
    for (std::size_t i = 0; i < nnz; ++i) {
        if (!std::getline(in, line)) {
            throw std::runtime_error(
                    "mtx: end of file after " + std::to_string(i) + " edges, expected " + std::to_string(nnz));
        }

        spla::uint         u = 0;
        spla::uint         v = 0;
        std::uint32_t      w = 1;
        std::istringstream body(line);
        if (!(body >> u >> v)) {
            throw std::runtime_error("mtx: edge line must have u v [w]: " + line);
        }
        if (!(body >> w)) {
            w = 1;
        }

        if (u == 0 || v == 0 || u > n || v > n) {
            throw std::runtime_error("mtx: vertices must be in 1..n: " + line);
        }
        u -= 1;
        v -= 1;

        if (u != v) {
            if (w > EDGE_ENCODE_MAX_WEIGHT) {
                throw std::runtime_error("mtx: weight too large for (32 - EDGE_ENCODE_SHIFT) high bits (max " +
                                         std::to_string(EDGE_ENCODE_MAX_WEIGHT) + "): " + line);
            }
            const std::uint32_t enc_uv = encode_edge(w, v);
            const std::uint32_t enc_vu = encode_edge(w, u);
            if (enc_uv == INF || enc_vu == INF) {
                throw std::runtime_error("mtx: encoded edge collides with absent-edge fill value: " + line);
            }
            A->set_uint(u, v, enc_uv);
            A->set_uint(v, u, enc_vu);
        }
    }

    return A;
}

std::vector<MstEdge> boruvka_mst(const spla::ref_ptr<spla::Matrix>& A) {
    const spla::uint     n = A->get_n_rows();
    std::vector<MstEdge> result;
    if (n <= 1) {
        return result;
    }

    spla::ref_ptr<spla::Descriptor> desc = spla::Descriptor::make();
    spla::ref_ptr<spla::Scalar>     inf  = spla::Scalar::make_uint(INF);

    // Graph S ⊆ A: only edges whose endpoints lie in different DSU components (rebuilt after merges).
    // Initialized as element-wise copy of A via e-wise add with FIRST (same weights).
    spla::ref_ptr<spla::Matrix> S = spla::Matrix::make(n, n, spla::UINT);
    S->set_format(spla::FormatMatrix::AccCsr);
    (void) S->set_fill_value(inf);
    (void) spla::exec_m_eadd(S, A, A, spla::FIRST_UINT, desc);

    spla::ref_ptr<spla::Matrix> S_new = spla::Matrix::make(n, n, spla::UINT);
    S_new->set_format(spla::FormatMatrix::AccCsr);
    (void) S_new->set_fill_value(inf);

    Dsu dsu(n);

    // Per component root r: best outgoing edge this phase (u -> v, decoded w). Empty if w == INF.
    std::vector<MstEdge> min_edge(n);
    for (MstEdge& e : min_edge) {
        e.w = INF;
    }

    // Row minima of S after exec_m_reduce_by_row (one packed value per vertex).
    spla::ref_ptr<spla::Vector> row_min = spla::Vector::make(n, spla::UINT);
    (void) row_min->set_fill_value(inf);

    bool S_is_empty = false;

    while (!S_is_empty) {
        // --- Phase 1: for every vertex v, cheapest outgoing edge in S (SPLA row-wise MIN on packed codes). ---
        for (MstEdge& e : min_edge) {
            e.w = INF;
        }
        (void) row_min->fill_with(inf);

        (void) spla::exec_m_reduce_by_row(row_min, S, spla::MIN_UINT, inf, desc);

        // --- Phase 2: for each component (by root), pick the best edge among rows whose root is that component. ---
        for (spla::uint v = 0; v < n; ++v) {
            spla::T_UINT       code = 0;
            const spla::Status st   = row_min->get_uint(v, code);
            if (st != spla::Status::Ok || code == INF) {
                continue;
            }
            const spla::uint root = dsu.find(v);
            std::uint32_t    w_dec;
            spla::uint       dst;
            decode_edge(code, w_dec, dst);
            const MstEdge& cur    = min_edge[root];
            const bool     better = w_dec < cur.w || (w_dec == cur.w && dst < cur.v);
            if (better) {
                min_edge[root].u = v;
                min_edge[root].v = dst;
                min_edge[root].w = w_dec;
            }
        }

        // --- Phase 3: for each component root, add its chosen edge to the MST and merge endpoints in DSU. ---
        bool added_this_phase = false;

        for (spla::uint i = 0; i < n; ++i) {
            if (!dsu.is_representative(i)) {
                continue;
            }
            const MstEdge& e = min_edge[i];
            if (e.w == INF) {
                continue;
            }
            const spla::uint ru = dsu.find(e.u);
            const spla::uint rv = dsu.find(e.v);
            if (ru == rv) {
                continue;
            }
            const spla::uint lo = e.u < e.v ? e.u : e.v;
            const spla::uint hi = e.u < e.v ? e.v : e.u;
            result.push_back(MstEdge{lo, hi, e.w});
            added_this_phase = true;
            dsu.unite(e.u, e.v);
        }

        if (!added_this_phase) break;

        // --- Phase 4: drop intra-component edges from S (they cannot be chosen later). Rebuild COO into S_new; swap. ---
        (void) S_new->clear();
        spla::ref_ptr<spla::MemView> rows_view;
        spla::ref_ptr<spla::MemView> cols_view;
        spla::ref_ptr<spla::MemView> vals_view;
        S->read(rows_view, cols_view, vals_view);
        const std::size_t nnz_s = vals_view->get_size() / sizeof(std::uint32_t);
        const auto*       rows  = static_cast<const spla::uint*>(rows_view->get_buffer());
        const auto*       cols  = static_cast<const spla::uint*>(cols_view->get_buffer());
        const auto*       vals =
                reinterpret_cast<const std::uint32_t*>(vals_view->get_buffer());

        // After swap, S holds cross-component edges only; true iff no set_uint was called on S_new.
        S_is_empty = true;
        for (std::size_t k = 0; k < nnz_s; ++k) {
            const spla::uint r = rows[k];
            const spla::uint c = cols[k];
            if (dsu.find(r) != dsu.find(c)) {
                (void) S_new->set_uint(r, c, vals[k]);
                S_is_empty = false;
            }
        }
        std::swap(S, S_new);
    }

    return result;
}
