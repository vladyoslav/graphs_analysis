#pragma once

#include <spla.hpp>

#include <bit>
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

// Lowest shift bits = neighbor index, upper = weight. MIN order matches (weight, neighbor).
// For an n×n graph, shift == edge_encode_shift_for_n(n). load_graph encodes with this rule; boruvka_* recomputes from get_n_rows().
inline std::uint32_t edge_encode_shift_for_n(spla::uint n) {
    const std::uint32_t key = (n <= 1u) ? 0u : static_cast<std::uint32_t>(n - 1u);
    return std::bit_width(key);
}

inline std::uint32_t edge_index_mask(std::uint32_t shift) {
    return shift == 0u ? 0u : ((std::uint32_t{1} << shift) - std::uint32_t{1});
}

inline std::uint32_t edge_max_weight_for_shift(std::uint32_t shift) {
    return INF >> shift;
}

inline std::uint32_t encode_edge(std::uint32_t weight, spla::uint neighbor_index, std::uint32_t shift) {
    const std::uint32_t mask = edge_index_mask(shift);
    return (weight << shift) | (static_cast<std::uint32_t>(neighbor_index) & mask);
}

inline void decode_edge(std::uint32_t code, std::uint32_t& weight, spla::uint& neighbor_index, std::uint32_t shift) {
    weight         = code >> shift;
    neighbor_index = static_cast<spla::uint>(code & edge_index_mask(shift));
}

// Square Matrix Market (.mtx): preamble with %-lines; then "n m nnz"; then nnz lines "u v [w]" (1-based u,v; w optional uint32, default 1).
// UINT adjacency stores encode_edge(w, other_vertex, edge_encode_shift_for_n(n)); fill INF = no edge. Undirected. Self-loops: no matrix entry, line still counts.
// Requires n such that edge_encode_shift_for_n(n) <= 31 (roughly n <= 2^31), and weights <= edge_max_weight_for_shift(edge_encode_shift_for_n(n)).
// encoded value INF (fill) is rejected per edge.
// Throws std::runtime_error on parse / IO errors.
// storage_format: AccCsr for OpenCL-capable runs; CpuCsr when acceleration is disabled (see main --cpu-only).
spla::ref_ptr<spla::Matrix> load_graph(const std::string& mtx_path,
                                        spla::FormatMatrix      storage_format = spla::FormatMatrix::AccCsr);

// CPU classic Borůvka on COO (same packing as load_graph: shift from A->get_n_rows()).
std::vector<MstEdge> boruvka_mst_classic(const spla::ref_ptr<spla::Matrix>& A);

// SPLA-accelerated Borůvka (same semantics as classic).
// working_format: same family as A (typically AccCsr or CpuCsr from load_graph).
std::vector<MstEdge> boruvka_mst(const spla::ref_ptr<spla::Matrix>& A,
                                 spla::FormatMatrix                working_format = spla::FormatMatrix::AccCsr);
