#include <algorithm>
#include <filesystem>
#include <gtest/gtest.h>
#include <ostream>
#include <string>
#include <tuple>
#include <vector>

#include <spla.hpp>

#include "lib.hpp"

namespace {

    std::filesystem::path graphs_directory() {
        // This file: boruvka/boruvka_spla/test/lib_test.cpp → graphs live in boruvka/graphs/
        return std::filesystem::path(__FILE__).parent_path().parent_path().parent_path() / "graphs";
    }

    std::string graph_path(const char* mtx_filename) {
        return (graphs_directory() / mtx_filename).string();
    }

    auto edge_key(const MstEdge& e) { return std::make_tuple(e.u, e.v, e.w); }

    void sort_edges(std::vector<MstEdge>& e) {
        std::sort(e.begin(), e.end(), [](const MstEdge& a, const MstEdge& b) {
            return edge_key(a) < edge_key(b);
        });
    }

    void expect_same_edge_set(std::vector<MstEdge> got, std::vector<MstEdge> want) {
        sort_edges(got);
        sort_edges(want);
        ASSERT_EQ(got.size(), want.size());
        for (std::size_t i = 0; i < got.size(); ++i) {
            EXPECT_EQ(got[i].u, want[i].u);
            EXPECT_EQ(got[i].v, want[i].v);
            EXPECT_EQ(got[i].w, want[i].w);
        }
    }

}// namespace

struct LoadGraphCase {
    const char* id;
    const char* mtx_filename;
    spla::uint  expected_n;
};

struct BoruvkaCase {
    const char*          id;
    const char*          mtx_filename;
    spla::uint           expected_n;
    std::vector<MstEdge> expected_mst;
};

void PrintTo(const LoadGraphCase& c, std::ostream* os) {
    *os << c.id;
}

void PrintTo(const BoruvkaCase& c, std::ostream* os) {
    *os << c.id;
}

class LoadGraphParameterized : public ::testing::TestWithParam<LoadGraphCase> {
protected:
    void SetUp() override {
        spla::Library* lib = spla::Library::get();
        lib->set_platform(0);
        lib->set_device(0);
        lib->set_queues_count(1);
    }

    void TearDown() override { spla::Library::get()->finalize(); }
};

class BoruvkaParameterized : public ::testing::TestWithParam<BoruvkaCase> {
protected:
    void SetUp() override {
        spla::Library* lib = spla::Library::get();
        lib->set_platform(0);
        lib->set_device(0);
        lib->set_queues_count(1);
    }

    void TearDown() override { spla::Library::get()->finalize(); }
};

// Expected MST/MSF edges (0-based u<v), weights as uint32 after load_graph (see .mtx comments); verified by hand (Kruskal), not by this code.
INSTANTIATE_TEST_SUITE_P(
        All,
        LoadGraphParameterized,
        ::testing::Values(
                LoadGraphCase{"test_graph", "test_graph.mtx", 5},
                LoadGraphCase{"unweighted_path4", "unweighted_path4.mtx", 4},
                LoadGraphCase{"two_components_path", "two_components_path.mtx", 6}),
        [](const ::testing::TestParamInfo<LoadGraphCase>& info) {
            return std::string(info.param.id);
        });

INSTANTIATE_TEST_SUITE_P(
        All,
        BoruvkaParameterized,
        ::testing::Values(
                BoruvkaCase{
                        "test_graph",
                        "test_graph.mtx",
                        5,
                        // Optimum MST weight 12: (1,2,1)(2,3,2)(3,4,5)(4,5,4) in 1-based from test_graph.mtx
                        {{0, 1, 1}, {1, 2, 2}, {2, 3, 5}, {3, 4, 4}},
                },
                BoruvkaCase{
                        "unweighted_path4",
                        "unweighted_path4.mtx",
                        4,
                        {{0, 1, 1}, {1, 2, 1}, {2, 3, 1}},
                },
                BoruvkaCase{
                        "two_components_path",
                        "two_components_path.mtx",
                        6,
                        {{0, 1, 1}, {1, 2, 1}, {3, 4, 1}, {4, 5, 1}},
                }),
        [](const ::testing::TestParamInfo<BoruvkaCase>& info) {
            return std::string(info.param.id);
        });

TEST_P(LoadGraphParameterized, ReadsMtxDimensions) {
    const LoadGraphCase&        p = GetParam();
    spla::ref_ptr<spla::Matrix> g = load_graph(graph_path(p.mtx_filename));
    ASSERT_TRUE(g);
    EXPECT_EQ(g->get_n_rows(), p.expected_n);
    EXPECT_EQ(g->get_n_cols(), p.expected_n);
}

TEST_P(BoruvkaParameterized, MstEqualsExpected) {
    const BoruvkaCase&          p = GetParam();
    spla::ref_ptr<spla::Matrix> g = load_graph(graph_path(p.mtx_filename));
    ASSERT_TRUE(g);
    ASSERT_EQ(g->get_n_rows(), p.expected_n);

    std::vector<MstEdge> mst = boruvka_mst(g);
    for (const MstEdge& e : mst) {
        EXPECT_LT(e.u, e.v);
    }
    expect_same_edge_set(mst, p.expected_mst);
}
