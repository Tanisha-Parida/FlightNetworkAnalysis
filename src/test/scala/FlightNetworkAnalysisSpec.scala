package flightnetwork

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.Edge
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class FlightNetworkAnalysisSpec extends AnyFunSuite with Matchers with BeforeAndAfterAll {

  private var sc: SparkContext = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    val conf = new SparkConf()
      .setAppName("FlightNetworkAnalysisSpec")
      .setMaster("local[*]")
      .set("spark.driver.host", "localhost")
    sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
  }

  override def afterAll(): Unit = {
    if (sc != null) {
      sc.stop()
    }
    super.afterAll()
  }

  test("FR-02: parseLine properly parses valid rows, ignores headers and blank lines, and skips non-numeric rows") {
    // Valid data row
    FlightNetworkAnalysis.parseLine("DELHI,MUMBAI,150") shouldBe Some(FlightRoute("DELHI", "MUMBAI", 150))

    // Header row (case-insensitive) should return None
    FlightNetworkAnalysis.parseLine("source,destination,flights") shouldBe None
    FlightNetworkAnalysis.parseLine("SOURCE,DESTINATION,FLIGHTS") shouldBe None

    // Blank and whitespace lines should return None
    FlightNetworkAnalysis.parseLine("") shouldBe None
    FlightNetworkAnalysis.parseLine("   \t  \n") shouldBe None

    // Malformed rows (< 3 fields) should return None
    FlightNetworkAnalysis.parseLine("DELHI,MUMBAI") shouldBe None

    // Non-numeric flight count should print warning to stderr and return None without throwing exception
    FlightNetworkAnalysis.parseLine("DELHI,MUMBAI,not_a_number") shouldBe None
    FlightNetworkAnalysis.parseLine("DELHI,MUMBAI,12.5") shouldBe None
  }

  test("FR-02 & FR-04: Header, blank lines, and non-numeric rows create no vertices or edges") {
    val messyInput = sc.parallelize(Seq(
      "source,destination,flights",
      "",
      "   ",
      "DELHI,MUMBAI,INVALID_COUNT",
      "SHORT_ROW",
      "DELHI,MUMBAI,100"
    ))

    val routes = messyInput.flatMap(line => FlightNetworkAnalysis.parseLine(line))
    val (nameToId, vertices) = FlightNetworkAnalysis.buildIds(routes)
    val graph = FlightNetworkAnalysis.buildGraph(routes, nameToId, vertices)

    // Only the single valid row "DELHI,MUMBAI,100" should remain
    graph.vertices.count() shouldBe 2
    graph.edges.count() shouldBe 1
    routes.collect() should contain theSameElementsAs Seq(FlightRoute("DELHI", "MUMBAI", 100))
  }

  test("FR-06 & FR-08: On data/sample3.csv, degrees must be DELHI=2, MUMBAI=2, BENGALURU=1, HYDERABAD=1 and form ONE connected component") {
    val routes = FlightNetworkAnalysis.readRoutes(sc, "data/sample3.csv")
    val (nameToId, vertices) = FlightNetworkAnalysis.buildIds(routes)
    val graph = FlightNetworkAnalysis.buildGraph(routes, nameToId, vertices)

    // Compute degrees mapped to airport names
    val degreeMap = graph.vertices
      .leftOuterJoin(graph.degrees)
      .map { case (_, (name, degOpt)) => (name, degOpt.getOrElse(0)) }
      .collect()
      .toMap

    // Assert expected degrees
    degreeMap("DELHI") shouldBe 2
    degreeMap("MUMBAI") shouldBe 2
    degreeMap("BENGALURU") shouldBe 1
    degreeMap("HYDERABAD") shouldBe 1

    // Connected components: direction ignored, all 4 airports must belong to a single component
    val ccGraph = graph.connectedComponents()
    val distinctComponents = ccGraph.vertices.map(_._2).distinct().collect()
    distinctComponents.length shouldBe 1
  }

  test("FR-03, FR-04, FR-05: Vertex count equals unique airports; edge count equals valid rows on data/flights.csv") {
    val routes = FlightNetworkAnalysis.readRoutes(sc, "data/flights.csv")
    val (nameToId, vertices) = FlightNetworkAnalysis.buildIds(routes)
    val graph = FlightNetworkAnalysis.buildGraph(routes, nameToId, vertices)

    // 8 Indian city airports: DELHI, MUMBAI, BENGALURU, CHENNAI, HYDERABAD, KOLKATA, AHMEDABAD, GOA
    graph.vertices.count() shouldBe 8

    // 12 valid flight routes
    graph.edges.count() shouldBe 12

    // All airports should be present
    val expectedAirports = Set("DELHI", "MUMBAI", "BENGALURU", "CHENNAI", "HYDERABAD", "KOLKATA", "AHMEDABAD", "GOA")
    val actualAirports = graph.vertices.map(_._2).collect().toSet
    actualAirports shouldBe expectedAirports
  }

  test("Design Decision: Duplicate routes between the same airports are kept as separate edges") {
    val duplicateInput = sc.parallelize(Seq(
      FlightRoute("DELHI", "MUMBAI", 100),
      FlightRoute("DELHI", "MUMBAI", 200)
    ))

    val (nameToId, vertices) = FlightNetworkAnalysis.buildIds(duplicateInput)
    val graph = FlightNetworkAnalysis.buildGraph(duplicateInput, nameToId, vertices)

    graph.vertices.count() shouldBe 2
    graph.edges.count() shouldBe 2
    val edges = graph.edges.collect()
    edges.map(_.attr) should contain theSameElementsAs Seq(100, 200)
  }

  test("Interactive Text Input: The sample text gives 4 vertices and 3 edges") {
    val sampleText =
      """source,destination,flights
        |DELHI,MUMBAI,120
        |DELHI,BENGALURU,95
        |MUMBAI,HYDERABAD,70""".stripMargin

    val (reportText, summary, warnings) = FlightNetworkAnalysis.analyzeText(sc, sampleText)
    summary.vertexCount shouldBe 4
    summary.edgeCount shouldBe 3
    warnings shouldBe empty
    reportText should include("Total Airports (Vertices) : 4")
    reportText should include("Total Flight Routes (Edges): 3")
  }

  test("Interactive Text Input: An empty input gives a clear error, not an unhandled exception") {
    val emptyInput = "   \n\n  \t  "
    val ex = intercept[IllegalArgumentException] {
      FlightNetworkAnalysis.analyzeText(sc, emptyInput)
    }
    ex.getMessage should include("Input is empty or contains no valid flight routes")
  }

  test("Interactive Text Input: A row with a non-numeric count is skipped and reported as a warning") {
    val textWithInvalidRow =
      """source,destination,flights
        |DELHI,MUMBAI,120
        |DELHI,BENGALURU,bad_number
        |MUMBAI,HYDERABAD,70""".stripMargin

    val (_, summary, warnings) = FlightNetworkAnalysis.analyzeText(sc, textWithInvalidRow)
    // Only 2 valid routes (DELHI->MUMBAI and MUMBAI->HYDERABAD) across 3 airports
    summary.vertexCount shouldBe 3
    summary.edgeCount shouldBe 2
    warnings should not be empty
    warnings.head should include("Skipping row with non-numeric flight count")
  }
}
