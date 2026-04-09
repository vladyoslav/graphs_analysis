#include "lib.hpp"

#include <algorithm>
#include <cstdint>
#include <fstream>
#include <numeric>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace {

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

spla::ref_ptr<spla::Matrix> load_graph(const std::string& mtx_path, spla::FormatMatrix storage_format) {
    std::ifstream in(mtx_path);
    if (!in.is_open()) {
        throw std::runtime_error("mtx: cannot open " + mtx_path);
    }

    spla::uint  n   = 0;
    std::size_t nnz = 0;
    // Phase: parse dimensions (n, nnz) from the first non-comment line.
    parse_size_line(read_first_data_line(in), n, nnz);

    const std::uint32_t shift = edge_encode_shift_for_n(n);
    if (shift > 31u) {
        throw std::runtime_error("mtx: n too large for 32-bit packed (weight, neighbor) encoding (n=" +
                                 std::to_string(n) + ")");
    }
    const std::uint32_t max_w = edge_max_weight_for_shift(shift);

    // Phase: allocate n×n UINT matrix; absent edges use INF (must not collide with encode_edge).
    spla::ref_ptr<spla::Matrix> A = spla::Matrix::make(n, n, spla::UINT);
    if (!A) {
        throw std::runtime_error("mtx: failed to allocate adjacency matrix");
    }
    A->set_format(storage_format);
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
            if (w > max_w) {
                throw std::runtime_error("mtx: weight too large for encode shift " + std::to_string(shift) +
                                         " (max " + std::to_string(max_w) + "): " + line);
            }
            const std::uint32_t enc_uv = encode_edge(w, v, shift);
            const std::uint32_t enc_vu = encode_edge(w, u, shift);
            if (enc_uv == INF || enc_vu == INF) {
                throw std::runtime_error("mtx: encoded edge collides with absent-edge fill value: " + line);
            }
            A->set_uint(u, v, enc_uv);
            A->set_uint(v, u, enc_vu);
        }
    }

    return A;
}

std::vector<MstEdge> boruvka_mst(const spla::ref_ptr<spla::Matrix>& A, spla::FormatMatrix working_format) {
    const spla::uint     n         = A->get_n_rows();
    const std::uint32_t  enc_shift = edge_encode_shift_for_n(n);
    std::vector<MstEdge> result;
    if (n <= 1) {
        return result;
    }

    auto desc = spla::Descriptor::make();
    auto inf  = spla::Scalar::make_uint(INF);

    // S: working matrix with packed (weight, neighbor) edges; rebuilt each iteration
    // to contain only cross-component edges.
    auto S = spla::Matrix::make(n, n, spla::UINT);
    S->set_format(working_format);
    (void) S->set_fill_value(inf);
    (void) spla::exec_m_eadd(S, A, A, spla::FIRST_UINT, desc);

    auto S_new = spla::Matrix::make(n, n, spla::UINT);
    S_new->set_format(working_format);
    (void) S_new->set_fill_value(inf);

    // P: membership matrix — P[parent[i]][i] = 1.
    // Row `root` of P has 1s in columns belonging to that component.
    auto P = spla::Matrix::make(n, n, spla::UINT);
    P->set_format(working_format);

    // edge[i] = min packed (weight, neighbor) for vertex i  (GPU output)
    auto edge = spla::Vector::make(n, spla::UINT);
    (void) edge->set_fill_value(inf);

    // cedge[root] = min packed (weight, neighbor) for component root  (GPU output via P × edge)
    auto cedge = spla::Vector::make(n, spla::UINT);
    (void) cedge->set_fill_value(inf);

    // Dense mask for mxv (fill_value makes every position active with ALWAYS select)
    auto mask = spla::Vector::make(n, spla::UINT);
    (void) mask->set_fill_value(spla::Scalar::make_uint(1u));

    // parent[i] = root of component containing i.
    // Invariant: after path compression parent[i] == root for all i.
    // Root = minimum vertex id in the component.
    std::vector<spla::uint> parent(n);
    std::iota(parent.begin(), parent.end(), spla::uint{0});

    bool S_is_empty = false;

    while (!S_is_empty) {
        // ==== Step 1: Row-wise MIN via mxv (GPU) ====
        // edge[i] = min_j S(i,j).
        // Semiring (FIRST, MIN): FIRST ignores the vector value and takes S(i,j);
        // MIN reduces over the row.  Equivalent to reduce_by_row but has an OpenCL path.
        (void) edge->fill_with(inf);
        (void) spla::exec_mxv_masked(
                edge, mask, S, mask,
                spla::FIRST_UINT, spla::MIN_UINT, spla::ALWAYS_UINT,
                inf, desc);

        // ==== Step 2: Build membership matrix P[parent[i]][i] = 1 ====
        (void) P->clear();
        for (spla::uint i = 0; i < n; ++i) {
            (void) P->set_uint(parent[i], i, 1u);
        }

        // ==== Step 3: Aggregate at roots — cedge = P ×(SECOND,MIN) edge  (GPU) ====
        // SECOND(P[root][j], edge[j]) = edge[j]; MIN reduces over vertices in the component.
        (void) cedge->fill_with(inf);
        (void) spla::exec_mxv_masked(
                cedge, mask, P, edge,
                spla::SECOND_UINT, spla::MIN_UINT, spla::ALWAYS_UINT,
                inf, desc);

        // ==== Step 4: Select MST edges + update parent (CPU) ====

        // Bulk-read GPU vectors in two transfers.
        spla::ref_ptr<spla::MemView> edge_keys_view, edge_vals_view;
        (void) edge->read(edge_keys_view, edge_vals_view);
        const std::size_t edge_nnz  = edge_vals_view->get_size() / sizeof(std::uint32_t);
        const auto*       edge_keys = static_cast<const spla::uint*>(edge_keys_view->get_buffer());
        const auto*       edge_vals = reinterpret_cast<const std::uint32_t*>(edge_vals_view->get_buffer());

        spla::ref_ptr<spla::MemView> cedge_keys_view, cedge_vals_view;
        (void) cedge->read(cedge_keys_view, cedge_vals_view);
        const std::size_t cedge_nnz  = cedge_vals_view->get_size() / sizeof(std::uint32_t);
        const auto*       cedge_keys = static_cast<const spla::uint*>(cedge_keys_view->get_buffer());
        const auto*       cedge_vals = reinterpret_cast<const std::uint32_t*>(cedge_vals_view->get_buffer());

        // Materialize cedge into a CPU array indexed by root.
        std::vector<std::uint32_t> cedge_cpu(n, INF);
        for (std::size_t i = 0; i < cedge_nnz; ++i) {
            if (cedge_vals[i] != INF) {
                cedge_cpu[cedge_keys[i]] = cedge_vals[i];
            }
        }

        // For each root, find the source vertex that contributed the component minimum.
        // O(edge_nnz) single scan — avoids an inner loop per root.
        std::vector<spla::uint> comp_src(n, n);
        for (std::size_t k = 0; k < edge_nnz; ++k) {
            const spla::uint v = edge_keys[k];
            const spla::uint r = parent[v];
            if (edge_vals[k] != INF && edge_vals[k] == cedge_cpu[r]) {
                if (comp_src[r] == n || v < comp_src[r]) {
                    comp_src[r] = v;
                }
            }
        }

        // Iterate roots in ascending order so 2-cycles resolve deterministically:
        // both roots agree that parent[max] = min; the smaller root adds the edge,
        // the larger one is already demoted when reached.
        bool added = false;
        for (spla::uint r = 0; r < n; ++r) {
            if (parent[r] != r) continue;
            if (cedge_cpu[r] == INF) continue;

            std::uint32_t w;
            spla::uint    j_dest;
            decode_edge(cedge_cpu[r], w, j_dest, enc_shift);

            const spla::uint dest_root = parent[j_dest];
            if (r == dest_root) continue;

            const spla::uint mn = std::min(r, dest_root);
            const spla::uint mx = std::max(r, dest_root);
            parent[mx]          = mn;

            const spla::uint src = (comp_src[r] != n) ? comp_src[r] : r;
            const spla::uint lo  = std::min(src, j_dest);
            const spla::uint hi  = std::max(src, j_dest);
            result.push_back(MstEdge{lo, hi, w});
            added = true;
        }

        if (!added) break;

        // ==== Step 5: Path compression (CPU) ====
        // Flatten the parent forest: parent[i] = root for all i.
        // Repeated pointer jumping: parent[i] ← parent[parent[i]] until convergence.
        bool changed = true;
        while (changed) {
            changed = false;
            for (spla::uint i = 0; i < n; ++i) {
                const spla::uint gp = parent[parent[i]];
                if (gp != parent[i]) {
                    parent[i] = gp;
                    changed   = true;
                }
            }
        }

        // ==== Step 6: Remove intra-component edges — COO filter + rebuild ====
        // After path compression parent[i] is the root directly
        (void) S_new->clear();
        spla::ref_ptr<spla::MemView> rows_view, cols_view, vals_view;
        (void) S->read(rows_view, cols_view, vals_view);
        const std::size_t nnz_s = vals_view->get_size() / sizeof(std::uint32_t);
        const auto*       rows  = static_cast<const spla::uint*>(rows_view->get_buffer());
        const auto*       cols  = static_cast<const spla::uint*>(cols_view->get_buffer());
        const auto*       vals  = reinterpret_cast<const std::uint32_t*>(vals_view->get_buffer());

        S_is_empty = true;
        for (std::size_t k = 0; k < nnz_s; ++k) {
            if (parent[rows[k]] != parent[cols[k]]) {
                (void) S_new->set_uint(rows[k], cols[k], vals[k]);
                S_is_empty = false;
            }
        }
        std::swap(S, S_new);
    }

    return result;
}
