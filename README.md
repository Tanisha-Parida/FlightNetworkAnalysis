# FlightNetworkAnalysis (Apache Spark GraphX Prototype in Scala)

A clean, production-styled Apache Spark GraphX prototype in Scala demonstrating graph analytics on airline flight networks. Built as a student portfolio project designed for recruiters and engineering managers, prioritizing clean design, explicit documentation, and maintainability over clever shortcuts.

---

## 📋 Project Overview

The project models domestic flight routes between Indian city airports as a directed graph using **Apache Spark GraphX**:
- **Vertices (Airports)**: Identified by a unique `VertexId` (`Long`) with the airport name (e.g., `DELHI`, `MUMBAI`) stored as the vertex attribute (`String`).
- **Edges (Routes)**: Directed connections (`srcId -> dstId`) weighted by the number of scheduled flights (`Int`).

The analytics pipeline executes:
1. **Graph Summary**: Total vertex and edge counts.
2. **Degree Ranking**: In-degree + out-degree per airport, ranked descending.
3. **PageRank**: Identifying major transit hubs (damping factor = 0.85, convergence tolerance = `0.0001`).
4. **Connected Components**: Detecting isolated clusters or island sub-networks (direction ignored).

---

## 📁 Project Structure

```text
FlightNetworkAnalysis/
├── build.sbt                               # Build definition (Scala 2.12.18, Spark 3.5.1, ScalaTest)
├── project/
│   └── build.properties                   # sbt version (1.9.9)
├── data/
│   ├── flights.csv                        # 12 synthetic flight routes across 8 Indian airports
│   └── sample3.csv                        # Minimal 3-route benchmark dataset
├── src/
│   ├── main/
│   │   └── scala/
│   │       └── FlightNetworkAnalysis.scala # Core pipeline (readRoutes, buildIds, buildGraph, printReport)
│   └── test/
│       └── scala/
│           └── FlightNetworkAnalysisSpec.scala # Comprehensive ScalaTest validation suite
└── README.md                              # Architecture, decisions, and usage guide
```

---

## 🛠️ Technical Stack & Prerequisites

- **Language**: Scala `2.12.18`
- **Framework**: Apache Spark `3.5.1` (`spark-core`, `spark-sql`, `spark-graphx`)
- **Testing**: ScalaTest `3.2.18`
- **Build Tool**: sbt `1.9.x`
- **Target Runtime**: JDK 17
- **Deployment Mode**: Spark `local[*]` (standalone master)

---

## 🚀 How to Run

### 1. Run the Main Application
```bash
# Clone or navigate to the project directory
cd FlightNetworkAnalysis

# Run with default input (data/flights.csv)
sbt run

# Run with a custom CSV dataset (e.g., data/sample3.csv)
sbt "run data/sample3.csv"
```

### 2. Run the Unit Test Suite
```bash
sbt test
```

---

## 💡 Architecture & Design Decisions

### 1. Directed Edges
- **Why**: Aviation routes are inherently asymmetric. A flight departing Delhi to Mumbai (`DELHI -> MUMBAI`) is operationally separate from the return flight (`MUMBAI -> DELHI`). Preserving directionality is essential for computing transit flow, hub dominance via PageRank, and in/out degree ratios.
- **Connected Components**: While the graph is directed, connected components naturally ignore edge direction to determine whether travelers can navigate between all airports across the network.

### 2. Numeric Long Identifiers (`VertexId`)
- **Requirement**: GraphX mandates primitive 64-bit `Long` integers (`VertexId`) for high-performance memory representation and distributed partitioning.
- **Deterministic Mapping**: Airport names are collected and sorted alphabetically to generate deterministic ID mappings (`Map[String, VertexId]`).
- **Driver Memory Consideration**: In this student prototype, collecting the unique airport names to the driver via `.collect()` is safe and concise for hundreds of airports. In a web-scale graph with millions of vertices, an RDD-based approach like `rdd.zipWithUniqueId()` should be used to prevent driver OutOfMemory (OOM) errors.
- **Human-Readable Output**: All terminal output and analytics reports map `VertexId`s back to the original airport names (`FR-09`).

### 3. Duplicate Routes as Separate Edges
- **Decision**: If multiple rows exist with the same source and destination (e.g., morning and evening flights, or code-share routes), they are retained as separate parallel edges rather than collapsed prematurely.
- **Multigraph Support**: GraphX natively supports multigraphs. Retaining individual edges preserves granular route information and enables downstream filtering or edge-specific transformations.

### 4. Robust Parsing & Fault Tolerance (`FR-02`)
- Blank lines and whitespace are silently ignored.
- Header rows (case-insensitive `source,destination,flights`) are identified and skipped.
- Lines with non-numeric flight numbers or fewer than 3 fields emit a warning to `stderr` and are skipped without terminating or crashing the Spark pipeline.

---

## 🔬 Dataset Description

### `data/flights.csv` (Synthetic Benchmark)
Contains 12 synthetic route rows across 8 major Indian city airports:
- **Airports**: `DELHI`, `MUMBAI`, `BENGALURU`, `CHENNAI`, `HYDERABAD`, `KOLKATA`, `AHMEDABAD`, `GOA`
- **Reverse Route**: Includes `DELHI -> MUMBAI` (150 flights) and `MUMBAI -> DELHI` (140 flights) to verify directed edge handling.
- **Blank Line**: Includes an empty line to test sanitization.

### `data/sample3.csv` (Deterministic Minimal Benchmark)
```csv
DELHI,MUMBAI,120
DELHI,BENGALURU,95
MUMBAI,HYDERABAD,70
```
Used for unit testing degree counts and single-component connectivity.

---

## 📊 Actual Execution Output

The console outputs below reflect an actual live run with Apache Spark 3.5.1 and Scala 2.12.18 on OpenJDK 17.

### 1. Run on `data/sample3.csv` (`sbt "run data/sample3.csv"`)
```text
Running FlightNetworkAnalysis with input: data/sample3.csv

=================================================================
              FLIGHT NETWORK ANALYSIS REPORT
=================================================================

[Graph Summary]
  Total Airports (Vertices) : 4
  Total Flight Routes (Edges): 3

[Degree Ranking (In-Degree + Out-Degree)]
  Rank   Airport Name       Total Degree
  --------------------------------------
  1      DELHI                         2
  2      MUMBAI                        2
  3      BENGALURU                     1
  4      HYDERABAD                     1

[PageRank Ranking (Tolerance = 0.0001)]
  Rank   Airport Name         PageRank Score
  ------------------------------------------
  1      HYDERABAD                  1.459270
  2      BENGALURU                  0.940400
  3      MUMBAI                     0.940400
  4      DELHI                      0.659930

[Connected Components (Undirected)]
  Total Components Discovered: 1
  Component 1 (Representative ID 0): [BENGALURU, DELHI, HYDERABAD, MUMBAI]

=================================================================
```

### 2. Run on `data/flights.csv` (`sbt run`)
```text
Running FlightNetworkAnalysis with input: data/flights.csv

=================================================================
              FLIGHT NETWORK ANALYSIS REPORT
=================================================================

[Graph Summary]
  Total Airports (Vertices) : 8
  Total Flight Routes (Edges): 12

[Degree Ranking (In-Degree + Out-Degree)]
  Rank   Airport Name       Total Degree
  --------------------------------------
  1      DELHI                         5
  2      MUMBAI                        5
  3      BENGALURU                     3
  4      HYDERABAD                     3
  5      AHMEDABAD                     2
  6      CHENNAI                       2
  7      GOA                           2
  8      KOLKATA                       2

[PageRank Ranking (Tolerance = 0.0001)]
  Rank   Airport Name         PageRank Score
  ------------------------------------------
  1      DELHI                      1.833855
  2      HYDERABAD                  1.444148
  3      KOLKATA                    1.377523
  4      MUMBAI                     1.057017
  5      CHENNAI                    0.994079
  6      BENGALURU                  0.993238
  7      AHMEDABAD                  0.150070
  8      GOA                        0.150070

[Connected Components (Undirected)]
  Total Components Discovered: 1
  Component 1 (Representative ID 0): [AHMEDABAD, BENGALURU, CHENNAI, DELHI, GOA, HYDERABAD, KOLKATA, MUMBAI]

=================================================================
```

---

## 🧪 Test Suite Specifications (`FlightNetworkAnalysisSpec`)

The test suite validates all functional requirements:
1. **FR-02 Input Cleaning**: Header rows (`source,destination,flights`) and blank/whitespace rows are filtered out.
2. **FR-02 Fault Tolerance**: Rows with non-numeric flight numbers or missing columns log a warning to `stderr` and are skipped without crashing.
3. **FR-06 & FR-08 Minimal Benchmark (`sample3.csv`)**:
   - `DELHI`: Degree = 2
   - `MUMBAI`: Degree = 2
   - `BENGALURU`: Degree = 1
   - `HYDERABAD`: Degree = 1
   - All 4 airports strictly belong to 1 connected component.
4. **FR-03, FR-04, FR-05 Graph Size (`flights.csv`)**:
   - Exactly 8 unique vertices matching all Indian airports.
   - Exactly 12 valid directed edges.
5. **Multigraph Preservation**: Duplicate routes between the same airports produce 2 distinct edges in the graph.

---

## ⚠️ Limitations & Scope

- **Synthetic Data**: Flight volumes and routes are synthetic and created for educational algorithm verification.
- **No Performance Claims**: The application is tailored for algorithmic clarity, local prototyping (`local[*]`), and educational review, not distributed cluster benchmarking on terabyte-scale datasets.
