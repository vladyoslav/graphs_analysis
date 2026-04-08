import os
import sys
import urllib.request
import zipfile
import argparse
import yaml


def preview_mtx(path):
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            lines = []
            for _ in range(5):
                line = f.readline()
                if not line:
                    break
                lines.append(line.strip())
        print("    preview:")
        for line in lines:
            print(f"      {line}")
    except Exception as e:
        print(f"    [WARN] Could not preview file: {e}")


def convert_edges_to_mtx(edges_path, mtx_path):
    """
    Convert edge list file to MatrixMarket symmetric pattern format.
    Assumes each non-comment line contains at least two integers: u v
    """
    edges = []
    max_node = 0
    min_node = None

    with open(edges_path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            if line.startswith("%") or line.startswith("#"):
                continue

            parts = line.split()
            if len(parts) < 2:
                continue

            try:
                u = int(parts[0])
                v = int(parts[1])
            except ValueError:
                continue

            edges.append((u, v))
            max_node = max(max_node, u, v)
            if min_node is None:
                min_node = min(u, v)
            else:
                min_node = min(min_node, u, v)

    if not edges:
        raise RuntimeError(f"No valid edges found in {edges_path}")

    # If vertices are 0-based, shift to 1-based for MatrixMarket
    shift = 1 if min_node == 0 else 0

    with open(mtx_path, "w", encoding="utf-8") as out:
        out.write("%%MatrixMarket matrix coordinate pattern symmetric\n")
        out.write(f"{max_node + shift} {max_node + shift} {len(edges)}\n")
        for u, v in edges:
            out.write(f"{u + shift} {v + shift}\n")


def download_graph(name, url, graphs_dir):
    """Downloads and extracts a single graph. Returns True on success."""
    final_path = os.path.join(graphs_dir, f"{name}.mtx")

    # Skip if already downloaded
    if os.path.exists(final_path):
        print(f"  [SKIP] {name}.mtx already exists")
        return True

    print(f"  [DOWNLOAD] {name}...")
    zip_path = os.path.join(graphs_dir, f"{name}.zip")

    try:
        # Download zip archive
        urllib.request.urlretrieve(url, zip_path)

        # Extract .mtx if available, otherwise .edges
        with zipfile.ZipFile(zip_path, "r") as zf:
            names = zf.namelist()

            mtx_files = [f for f in names if f.lower().endswith(".mtx")]
            edges_files = [f for f in names if f.lower().endswith(".edges")]

            if mtx_files:
                extracted_file = mtx_files[0]
                zf.extract(extracted_file, graphs_dir)

                extracted_path = os.path.join(graphs_dir, extracted_file)
                os.makedirs(os.path.dirname(extracted_path), exist_ok=True)

                if extracted_path != final_path:
                    if os.path.exists(final_path):
                        os.remove(final_path)
                    os.replace(extracted_path, final_path)

            elif edges_files:
                extracted_file = edges_files[0]
                zf.extract(extracted_file, graphs_dir)

                extracted_path = os.path.join(graphs_dir, extracted_file)

                # Convert .edges -> .mtx
                convert_edges_to_mtx(extracted_path, final_path)

                # Remove extracted .edges file after conversion
                if os.path.exists(extracted_path):
                    os.remove(extracted_path)

            else:
                print(f"  [ERROR] No .mtx or .edges file found in archive for {name}")
                return False

        # Clean up zip
        os.remove(zip_path)
        print(f"  [OK] {name}.mtx")
        preview_mtx(final_path)
        return True

    except Exception as e:
        print(f"  [ERROR] {name}: {e}")
        if os.path.exists(zip_path):
            os.remove(zip_path)
        return False


def main():
    parser = argparse.ArgumentParser(description="Download graphs from Network Repository.")
    parser.add_argument(
        "graphs",
        help="Comma-separated graph names (e.g., 'minnesota,ca-GrQc') or 'all'"
    )
    parser.add_argument(
        "-d", "--dir",
        default=".",
        help="Output directory (default: current directory)"
    )

    args = parser.parse_args()
    graphs_dir = args.dir
    os.makedirs(graphs_dir, exist_ok=True)

    # Load YAML config from the same directory where this script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    yaml_path = os.path.join(script_dir, "graphs.yaml")

    if not os.path.exists(yaml_path):
        print(f"Error: Configuration file not found at {yaml_path}")
        sys.exit(1)

    with open(yaml_path, "r", encoding="utf-8") as f:
        graphs_db = yaml.safe_load(f)

    # Determine which graphs to download
    if args.graphs.lower() == "all":
        target_graphs = list(graphs_db.keys())
    else:
        requested = [g.strip() for g in args.graphs.split(",")]
        target_graphs = []
        for g in requested:
            if g in graphs_db:
                target_graphs.append(g)
            else:
                print(f"  [WARNING] Graph '{g}' not found in graphs.yaml. Skipping.")

    if not target_graphs:
        print("No valid graphs specified to download.")
        sys.exit(1)

    print(f"=== Downloading {len(target_graphs)} graph(s) to '{graphs_dir}' ===")

    failed = []
    for name in target_graphs:
        url = graphs_db[name]
        if not download_graph(name, url, graphs_dir):
            failed.append(name)

    print()
    if failed:
        print(f"=== ERROR: Failed to download: {', '.join(failed)} ===")
        sys.exit(1)

    print(f"=== Done. Graphs in '{graphs_dir}': ===")
    for f in sorted(os.listdir(graphs_dir)):
        if f.endswith(".mtx"):
            size = os.path.getsize(os.path.join(graphs_dir, f))
            if size > 1024 * 1024:
                print(f"  {f}  ({size / (1024*1024):.1f} MB)")
            else:
                print(f"  {f}  ({size / 1024:.1f} KB)")


if __name__ == "__main__":
    main()
    