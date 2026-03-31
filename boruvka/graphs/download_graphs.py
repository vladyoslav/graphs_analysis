import os
import sys
import urllib.request
import zipfile
import argparse
import yaml


def download_graph(name, url, graphs_dir):
    """Downloads and extracts a single graph. Returns True on success."""
    final_path = os.path.join(graphs_dir, f"{name}.mtx")

    if os.path.exists(final_path):
        print(f"  [SKIP] {name}.mtx already exists")
        return True

    print(f"  [DOWNLOAD] {name}...")
    zip_path = os.path.join(graphs_dir, f"{name}.zip")

    try:
        urllib.request.urlretrieve(url, zip_path)

        with zipfile.ZipFile(zip_path, "r") as zf:
            valid_extensions = (".mtx", ".edges")
            target_files = [f for f in zf.namelist() if f.endswith(valid_extensions)]

            if not target_files:
                print(f"  [ERROR] No .mtx or .edges file found in archive for {name}")
                return False

            extracted_file = target_files[0]
            zf.extract(extracted_file, graphs_dir)

            # Standardize filename to {name}.mtx regardless of original archive contents
            extracted_path = os.path.join(graphs_dir, extracted_file)
            if extracted_path != final_path:
                if os.path.exists(final_path):
                    os.remove(final_path)
                os.rename(extracted_path, final_path)

        os.remove(zip_path)
        print(f"  [OK] {name}.mtx")
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
    