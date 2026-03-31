#include <cstdint>
#include <gtest/gtest.h>
#include <spla.hpp>
#include <vector>

#include "lib.hpp"

#ifndef TEST_GRAPH_MTX_PATH
    #error TEST_GRAPH_MTX_PATH must be defined by CMake
#endif

namespace {

    // True iff mst has n-1 edges, each matches A[u,v] = encode_edge(w,v), every vertex appears, and
    // MST edges induce a connected graph (hence a spanning tree on n vertices).
    bool is_spanning_tree_matching_adjacency(const spla::ref_ptr<spla::Matrix>& A,
                                             const std::vector<MstEdge>&        mst,
                                             spla::uint                         n) {
        if (n <= 1) {
            return mst.empty();
        }
        if (mst.size() != static_cast<std::size_t>(n - 1)) {
            return false;
        }

        std::vector<unsigned char>           seen_vertex(n, 0);
        std::vector<std::vector<spla::uint>> adj(n);

        for (const MstEdge& e : mst) {
            if (e.u >= n || e.v >= n || e.u >= e.v) {
                return false;
            }
            std::uint32_t code = INF;
            if (A->get_uint(e.u, e.v, code) != spla::Status::Ok || code == INF) {
                return false;
            }
            if (code != encode_edge(e.w, e.v)) {
                return false;
            }
            seen_vertex[e.u] = 1;
            seen_vertex[e.v] = 1;
            adj[e.u].push_back(e.v);
            adj[e.v].push_back(e.u);
        }

        for (spla::uint i = 0; i < n; ++i) {
            if (!seen_vertex[i]) {
                return false;
            }
        }

        std::vector<unsigned char> vis(n, 0);
        std::vector<spla::uint>    st;
        st.push_back(0);
        vis[0] = 1;
        while (!st.empty()) {
            const spla::uint x = st.back();
            st.pop_back();
            for (spla::uint y : adj[x]) {
                if (!vis[y]) {
                    vis[y] = 1;
                    st.push_back(y);
                }
            }
        }
        for (spla::uint i = 0; i < n; ++i) {
            if (!vis[i]) {
                return false;
            }
        }
        return true;
    }

}// namespace

TEST(LoadGraph, ReadsTestGraphMtx) {
    spla::Library* library = spla::Library::get();
    library->set_platform(0);
    library->set_device(0);
    library->set_queues_count(1);

    spla::ref_ptr<spla::Matrix> g = load_graph(TEST_GRAPH_MTX_PATH);
    ASSERT_TRUE(g);
    EXPECT_EQ(g->get_n_rows(), 5u);
    EXPECT_EQ(g->get_n_cols(), 5u);

    library->finalize();
}

TEST(Boruvka, MstWeightOnTestGraph) {
    spla::Library* library = spla::Library::get();
    library->set_platform(0);
    library->set_device(0);
    library->set_queues_count(1);

    spla::ref_ptr<spla::Matrix> g = load_graph(TEST_GRAPH_MTX_PATH);
    ASSERT_TRUE(g);

    std::vector<MstEdge> mst = boruvka_mst(g);
    ASSERT_EQ(mst.size(), 4u);
    ASSERT_TRUE(is_spanning_tree_matching_adjacency(g, mst, g->get_n_rows()));

    std::uint64_t sum = 0;
    for (const MstEdge& e : mst) {
        EXPECT_LT(e.u, e.v);
        sum += e.w;
    }
    // Minimality on this instance: compare to known optimum (see test_graph.mtx comments).
    EXPECT_EQ(sum, 12u);

    library->finalize();
}
