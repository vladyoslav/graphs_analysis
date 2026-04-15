#!/usr/bin/env python3
"""
Converts DIMACS .gr graph format to Matrix Market .mtx format.

Usage:
    python gr_to_mtx.py input.gr output.mtx
"""

import sys
import argparse
from pathlib import Path
from collections import defaultdict


def parse_gr_file(gr_path):
    """
    Parses a .gr file in DIMACS format.
    
    Expected format:
        c <comment lines>
        p sp <num_vertices> <num_edges>
        a <src> <dst> <weight>
        ...
    
    Or alternative edge format:
        e <src> <dst> <weight>
    
    Returns:
        num_vertices (int)
        edges (list of tuples): [(src, dst, weight), ...]
    """
    num_vertices = 0
    num_edges = 0
    edges = []
    
    with open(gr_path, 'r') as f:
        for line in f:
            line = line.strip()
            
            if not line or line.startswith('c'):
                # Comment or empty line
                continue
            
            parts = line.split()
            
            if parts[0] == 'p':
                # Problem line: p sp <vertices> <edges>
                # or: p aux sp <vertices> <edges>
                if parts[1] == 'aux':
                    num_vertices = int(parts[3])
                    num_edges = int(parts[4])
                else:
                    num_vertices = int(parts[2])
                    num_edges = int(parts[3])
            
            elif parts[0] == 'a' or parts[0] == 'e':
                # Arc/Edge line: a <src> <dst> <weight>
                src = int(parts[1])
                dst = int(parts[2])
                weight = int(parts[3]) if len(parts) > 3 else 1
                
                edges.append((src, dst, weight))
    
    return num_vertices, edges


def write_mtx_file(mtx_path, num_vertices, edges, symmetric=True):
    """
    Writes edges to Matrix Market format.
    
    Args:
        mtx_path: output file path
        num_vertices: number of vertices
        edges: list of (src, dst, weight) tuples
        symmetric: if True, writes header as "symmetric" (undirected graph)
    """
    
    # Deduplicate edges if symmetric
    if symmetric:
        edge_set = set()
        for src, dst, weight in edges:
            # Store edge as (min, max, weight) to avoid duplicates
            u, v = (src, dst) if src < dst else (dst, src)
            edge_set.add((u, v, weight))
        edges = sorted(edge_set)
    
    num_edges = len(edges)
    
    with open(mtx_path, 'w') as f:
        # Header
        symmetry = "symmetric" if symmetric else "general"
        f.write(f"%%MatrixMarket matrix coordinate pattern {symmetry}\n")
        f.write(f"% Converted from .gr format\n")
        
        # Size line
        f.write(f"{num_vertices} {num_vertices} {num_edges}\n")
        
        # Edges
        for src, dst, weight in edges:
            # MTX format: row col [value]
            # For pattern matrices, no value is written
            # If you want weighted, use "integer" or "real" instead of "pattern"
            f.write(f"{src} {dst}\n")


def write_weighted_mtx_file(mtx_path, num_vertices, edges, symmetric=True):
    """
    Writes edges to Matrix Market format with weights.
    """
    
    # Deduplicate edges if symmetric
    if symmetric:
        edge_dict = {}
        for src, dst, weight in edges:
            u, v = (src, dst) if src < dst else (dst, src)
            # Keep minimum weight if duplicate (or choose another strategy)
            if (u, v) not in edge_dict or weight < edge_dict[(u, v)]:
                edge_dict[(u, v)] = weight
        edges = [(u, v, w) for (u, v), w in sorted(edge_dict.items())]
    
    num_edges = len(edges)
    
    with open(mtx_path, 'w') as f:
        # Header for weighted graph
        symmetry = "symmetric" if symmetric else "general"
        f.write(f"%%MatrixMarket matrix coordinate integer {symmetry}\n")
        f.write(f"% Converted from .gr format\n")
        
        # Size line
        f.write(f"{num_vertices} {num_vertices} {num_edges}\n")
        
        # Edges with weights
        for src, dst, weight in edges:
            f.write(f"{src} {dst} {weight}\n")


def main():
    parser = argparse.ArgumentParser(
        description='Convert DIMACS .gr format to Matrix Market .mtx format'
    )
    parser.add_argument('input', type=str, help='Input .gr file path')
    parser.add_argument('output', type=str, help='Output .mtx file path')
    parser.add_argument('--directed', action='store_true', 
                       help='Treat graph as directed (default: undirected/symmetric)')
    parser.add_argument('--weighted', action='store_true',
                       help='Include edge weights in output (default: pattern/unweighted)')
    
    args = parser.parse_args()
    
    input_path = Path(args.input)
    output_path = Path(args.output)
    
    # Validate input
    if not input_path.exists():
        print(f"Error: Input file not found: {input_path}", file=sys.stderr)
        sys.exit(1)
    
    if not input_path.is_file():
        print(f"Error: Input is not a file: {input_path}", file=sys.stderr)
        sys.exit(1)
    
    # Parse .gr file
    print(f"Reading {input_path}...")
    num_vertices, edges = parse_gr_file(input_path)
    
    print(f"Found {num_vertices} vertices and {len(edges)} edges")
    
    # Write .mtx file
    print(f"Writing to {output_path}...")
    symmetric = not args.directed
    
    if args.weighted:
        write_weighted_mtx_file(output_path, num_vertices, edges, symmetric)
    else:
        write_mtx_file(output_path, num_vertices, edges, symmetric)
    
    print(f"✅ Conversion complete!")
    print(f"   Vertices: {num_vertices}")
    print(f"   Edges: {len(edges)}")
    print(f"   Type: {'symmetric' if symmetric else 'general'}")
    print(f"   Weighted: {args.weighted}")


if __name__ == '__main__':
    main()
    