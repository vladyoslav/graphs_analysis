---
marp: true
theme: default
paginate: true
size: 16:9
---

# Борувка на SPLA и Apache Spark. Результаты

Шальнев Владислав, Кадочников Даниил

---

# Характеристики машины

**ОС:** Ubuntu 24.04 LTS

**CPU:** 10th Gen Intel Core i5-10300H @ 2.50 GHz
- Hyper-Threading включен
- L1: 256 КБ, L2: 1 МБ, L3: 8 МБ

**GPU:** NVIDIA GeForce GTX 1650 Ti Mobile
- 4 ГБ VRAM GDDR6
- драйвер 580.126.09
- CUDA Toolkit 13.0.97

**RAM:** 16 ГБ DDR4, 3200 МГц

---

# Программное обеспечение

- gcc 13.3.0
- OpenJDK 11.0.30
- SPLA (commit b81d1e2)
- Apache Spark 3.5.1
- OpenCL 3.0
- Intel OpenCL Runtime 26.05.37020.3
- CMake 3.28.3

---

# Данные

<style scoped>table > tbody > tr > td { width: 1% } table { margin: 40px 0; }</style>

| Граф | Количество вершин | Количество рёбер |
|:------|:------|:-------|
| amazon0601 | 403 394 | 2 443 408 |
| citationCiteseer | 268 495 | 1 156 647 |
| coPapersDBLP | 540 486 | 15 245 729 |
| in-2004 | 1 382 908 | 13 591 473 |
| cit-Patents | 3 774 768 | 16 518 947 |

Источники: [A High-Performance MST Implementation for GPUs 2023](https://userweb.cs.txstate.edu/~ag1548/_static/sc23.pdf), [SuiteSparse Matrix Collection](https://sparse.tamu.edu/)

---

# Эксперимент 1

**Цель:** сравнить время работы алгоритма Борувки на SPLA (CPU, NVIDIA OpenCL и Intel OpenCL) и Apache Spark на реальных графах разного размера

---


# Ход экспериментов

- 20 запусков алгоритма на каждом графе и библиотеке
- Вычисление среднего времени и доверительных интервалов
- Визуализация и анализ результатов

---

# Эксперимент 1

![](../graphics/1.png)

---

# Результаты

- SPLA CPU, Intel OpenCL и NVIDIA OpenCL показывают сопоставимое время — OpenCL не даёт заметного ускорения
- На малых графах SPLA быстрее Spark в 2-5 раз
- На больших графах разрыв сокращается

---

# Профилирование SPLA

<style scoped>img { width: 100%; height: 50vh; object-fit:cover; object-position:bottom; }</style>

![](../graphics/flamegraph_1.svg)

---

# Профилирование SPLA

<style scoped>img { width: 100%; height: 50vh; object-fit:cover; object-position:bottom; }</style>

![](../graphics/flamegraph_2.svg)

---

# Анализ результатов

- Использование OpenCL accelerator не дает ускорения, реализация `exec_m_reduce_by_row` на OpenCL отсутствует
- В SPLA отсутствует поддержка пользовательских типов
- Операции над матрицами на CPU, нет select, мало операций с масками