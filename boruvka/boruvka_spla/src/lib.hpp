#pragma once

#include <spla.hpp>

#include <cstdint>
#include <string>
#include <vector>

struct MstEdge {
    spla::uint    u = 0;
    spla::uint    v = 0;
    std::uint32_t w = 0;
};

// Packed-matrix fill, absent edge, and MIN-reduce identity (see load_graph, boruvka_mst).
inline constexpr std::uint32_t INF = UINT32_MAX;

// Legacy alias (same as no-edge / no candidate weight).
inline constexpr std::uint32_t MST_EDGE_SENTINEL_W = INF;

// Packed UINT edge value: neighbor index in low EDGE_ENCODE_SHIFT bits, weight in high (32 - EDGE_ENCODE_SHIFT) bits.
// MIN order: smaller weight first, then smaller neighbor index. Keep EDGE_ENCODE_SHIFT in [1, 31].
inline constexpr std::uint32_t EDGE_ENCODE_SHIFT = 20;
inline constexpr std::uint32_t EDGE_ENCODE_INDEX_MASK =
        (std::uint32_t{1} << EDGE_ENCODE_SHIFT) - std::uint32_t{1};
// Vertices are 0 .. n-1 with n-1 <= EDGE_ENCODE_INDEX_MASK.
inline constexpr spla::uint    EDGE_ENCODE_MAX_N      = spla::uint{1} << EDGE_ENCODE_SHIFT;
inline constexpr std::uint32_t EDGE_ENCODE_MAX_WEIGHT = INF >> EDGE_ENCODE_SHIFT;

inline std::uint32_t encode_edge(std::uint32_t weight, spla::uint neighbor_index) {
    return (weight << EDGE_ENCODE_SHIFT) | (static_cast<std::uint32_t>(neighbor_index) & EDGE_ENCODE_INDEX_MASK);
}

inline void decode_edge(std::uint32_t code, std::uint32_t& weight, spla::uint& neighbor_index) {
    weight         = code >> EDGE_ENCODE_SHIFT;
    neighbor_index = static_cast<spla::uint>(code & EDGE_ENCODE_INDEX_MASK);
}

// Square Matrix Market (.mtx): preamble with %-lines; then "n m nnz"; then nnz lines "u v [w]" (1-based u,v; w optional uint32, default 1).
// UINT adjacency stores encode_edge(w, other_vertex); fill INF = no edge. Undirected. Self-loops: no matrix entry, line still counts.
// Requires n <= EDGE_ENCODE_MAX_N and weights <= EDGE_ENCODE_MAX_WEIGHT; encoded value INF (fill) is rejected per edge.
// Throws std::runtime_error on parse / IO errors.
spla::ref_ptr<spla::Matrix> load_graph(const std::string& mtx_path);

// CPU classic Borůvka on COO from load_graph (decode_edge). Use for benchmarks vs. boruvka_mst.
std::vector<MstEdge> boruvka_mst_classic(const spla::ref_ptr<spla::Matrix>& A);

// Primary entry point; currently forwards to boruvka_mst_classic. Replace with SPLA path when ready.
std::vector<MstEdge> boruvka_mst(const spla::ref_ptr<spla::Matrix>& A);
