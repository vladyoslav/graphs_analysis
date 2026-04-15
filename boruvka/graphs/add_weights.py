import sys
from pathlib import Path

MAX_WEIGHT = 1023  # SPLA limit


def process_mtx(input_path: Path, output_path: Path):
    with open(input_path, 'r', encoding='utf-8', errors='ignore') as f:
        lines = f.readlines()

    header_lines = []
    size_line = None
    data_start_idx = 0
    
    # Собираем комментарии и находим строку размеров
    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith('%'):
            header_lines.append(line)
        else:
            # Первая строка без % — проверяем, размеры это или данные
            parts = stripped.split()
            if len(parts) == 3:
                try:
                    int(parts[0]), int(parts[1]), int(parts[2])
                    size_line = stripped
                    data_start_idx = i + 1
                    break
                except ValueError:
                    pass
            # Если не размеры — это уже данные
            data_start_idx = i
            break

    edges = []
    seen = set()

    for i in range(data_start_idx, len(lines)):
        line = lines[i].strip()
        if not line or line.startswith('%'):
            continue

        parts = line.split()
        if len(parts) < 2:
            continue

        try:
            src = int(parts[0])
            dst = int(parts[1])
        except ValueError:
            continue

        # Пропускаем петли
        if src == dst:
            continue

        # Нормализация для undirected (всегда min, max)
        if src > dst:
            src, dst = dst, src

        edge_key = (src, dst)
        if edge_key in seen:
            continue
        seen.add(edge_key)

        # Определяем вес
        if len(parts) >= 3:
            # Есть третья колонка — пытаемся прочитать вес
            try:
                weight = float(parts[2])
                # Если вес <= 0 или > MAX_WEIGHT — переопределяем
                if weight <= 0 or weight > MAX_WEIGHT:
                    weight = min(src, dst)
            except ValueError:
                # Не число — используем min(src, dst)
                weight = min(src, dst)
        else:
            # Нет третьей колонки — используем min(src, dst)
            weight = min(src, dst)

        # Дополнительная проверка на MAX_WEIGHT (на случай очень больших весов)
        if weight > MAX_WEIGHT:
            weight = (int(weight) % MAX_WEIGHT) + 1

        edges.append((src, dst, int(weight)))

    # Записываем результат
    with open(output_path, 'w', encoding='utf-8') as f:
        # Комментарии
        for hl in header_lines:
            f.write(hl)
        
        # Размеры
        if size_line:
            parts = size_line.split()
            f.write(f"{parts[0]} {parts[1]} {len(edges)}\n")
        else:
            # Вычисляем размеры из данных
            max_vertex = max(max(e[0], e[1]) for e in edges) if edges else 0
            f.write(f"{max_vertex} {max_vertex} {len(edges)}\n")
        
        # Рёбра
        for src, dst, weight in edges:
            f.write(f"{src} {dst} {weight}\n")

    print(f"[OK] {input_path.name} -> {output_path.name} ({len(edges)} edges)")


def main():
    if len(sys.argv) < 2:
        print("Usage: python add_weights.py <graph1,graph2,...>")
        print("       python add_weights.py all")
        print("Example: python add_weights.py road-luxembourg-osm,delaunay_n17,soc-LiveMocha")
        sys.exit(1)

    arg = sys.argv[1]

    if arg.lower() == 'all':
        mtx_files = list(Path('.').glob('*.mtx'))
    else:
        graph_names = [name.strip() for name in arg.split(',')]
        mtx_files = [Path(f"{name}.mtx") for name in graph_names]

    for mtx_file in mtx_files:
        if not mtx_file.exists():
            print(f"[SKIP] {mtx_file} not found")
            continue

        backup = mtx_file.with_suffix('.mtx.bak')
        if backup.exists():
            # Re-process from backup
            process_mtx(backup, mtx_file)
        else:
            mtx_file.rename(backup)
            process_mtx(backup, mtx_file)


if __name__ == "__main__":
    main()