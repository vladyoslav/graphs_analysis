// Classic CPU Borůvka on SPLA packed adjacency (COO from load_graph). For benchmarking vs. SPLA-accelerated path.

#include "lib.hpp"

#include <algorithm>
#include <cstdint>
#include <numeric>
#include <vector>

namespace {

    struct Candidate {
        bool          has = false;
        std::uint32_t w   = 0;
        spla::uint    lo  = 0;
        spla::uint    hi  = 0;
    };

    void relax_candidate(Candidate& c, spla::uint u, spla::uint v, std::uint32_t w) {
        if (w == INF) {
            return;
        }
        const spla::uint lo = std::min(u, v);
        const spla::uint hi = std::max(u, v);
        if (!c.has || w < c.w || (w == c.w && (lo < c.lo || (lo == c.lo && hi < c.hi)))) {
            c.has = true;
            c.w   = w;
            c.lo  = lo;
            c.hi  = hi;
        }
    }

    struct DsuCompact {
        std::vector<spla::uint> parent;
        std::vector<spla::uint> rankv;

        explicit DsuCompact(spla::uint n) : parent(n), rankv(n, 0) {
            std::iota(parent.begin(), parent.end(), spla::uint{0});
        }

        spla::uint find(spla::uint x) {
            if (parent[x] != x) {
                parent[x] = find(parent[x]);
            }
            return parent[x];
        }

        void unite(spla::uint a, spla::uint b) {
            a = find(a);
            b = find(b);
            if (a == b) {
                return;
            }
            if (rankv[a] < rankv[b]) {
                std::swap(a, b);
            }
            parent[b] = a;
            if (rankv[a] == rankv[b]) {
                ++rankv[a];
            }
        }
    };

}// namespace

std::vector<MstEdge> boruvka_mst_classic(const spla::ref_ptr<spla::Matrix>& A) {
    const spla::uint    n         = A->get_n_rows();
    const std::uint32_t enc_shift = edge_encode_shift_for_n(n);
    std::vector<MstEdge> result;
    if (n <= 1) {
        return result;
    }

    // Phases: for each component, one minimum-weight edge to another component; add forest; repeat.
    // COO scan only; no exec_m_reduce_by_row.

    DsuCompact dsu(n);

    spla::ref_ptr<spla::MemView> rows_view;
    spla::ref_ptr<spla::MemView> cols_view;
    spla::ref_ptr<spla::MemView> vals_view;
    A->read(rows_view, cols_view, vals_view);
    const std::size_t nnz  = vals_view->get_size() / sizeof(std::uint32_t);
    const auto*       rows = static_cast<const spla::uint*>(rows_view->get_buffer());
    const auto*       cols = static_cast<const spla::uint*>(cols_view->get_buffer());
    const auto*       vals =
            reinterpret_cast<const std::uint32_t*>(vals_view->get_buffer());

    while (result.size() < static_cast<std::size_t>(n - 1)) {
        std::vector<Candidate> best(n);

        for (std::size_t k = 0; k < nnz; ++k) {
            const spla::uint u = rows[k];
            const spla::uint v = cols[k];
            if (u >= v) {
                continue;
            }

            std::uint32_t w  = 0;
            spla::uint    nb = 0;
            decode_edge(vals[k], w, nb, enc_shift);
            if (w == INF || nb != v) {
                continue;
            }

            const spla::uint cu = dsu.find(u);
            const spla::uint cv = dsu.find(v);
            if (cu == cv) {
                continue;
            }
            relax_candidate(best[cu], u, v, w);
            relax_candidate(best[cv], u, v, w);
        }

        bool progressed = false;
        for (spla::uint c = 0; c < n; ++c) {
            if (dsu.find(c) != c) {
                continue;
            }
            const Candidate& cand = best[c];
            if (!cand.has) {
                continue;
            }
            if (dsu.find(cand.lo) == dsu.find(cand.hi)) {
                continue;
            }
            result.push_back(MstEdge{cand.lo, cand.hi, cand.w});
            dsu.unite(cand.lo, cand.hi);
            progressed = true;
            if (result.size() == static_cast<std::size_t>(n - 1)) {
                break;
            }
        }

        if (!progressed) {
            break;
        }
    }

    return result;
}
