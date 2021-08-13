package v1.controllers

import io.swagger.annotations._
import models.{Query, TranceObject}
import play.api.libs.json.Json
import play.api.mvc.{AbstractController, ControllerComponents}
import v1.repository.QueryRepository

import java.util.UUID
import javax.inject.Inject
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

import framework.common._
import framework.nrc._
import framework.plans.{CExpr, NRCTranslator}
import framework.plans.{BaseNormalizer, Finalizer}
import framework.plans.{Unnester, BaseOperatorANF}
import framework.plans.{JsonWriter => PJsonWriter}
import framework.examples.genomic._
import framework.optimize.Optimizer
import framework.generator.spark.{SparkDatasetGenerator,
  ZeppelinFactory,
  JsonWriter => ZJsonWriter}

@Api(value = "/nrccode")
class QueryController @Inject()(
                               cc: ControllerComponents,
                               queryRepository: QueryRepository
                               ) extends AbstractController (cc)
                                 with Materialization
                                 with MaterializeNRC
                                 with Shredding
                                 with NRCTranslator {


  // think about where these should go... these are the schemas!
  private val occur = new Occurrence{}
  private val cnum = new CopyNumber{}
  private val samps = new Biospecimen{}

  private val tbls = Map("occurrences" -> BagType(occur.occurmid_type),
               "copynumber" -> BagType(cnum.copyNumberType),
               "samples" -> BagType(samps.biospecType))

  private val parser = Parser(tbls)
  private val normalizer = new Finalizer(new BaseNormalizer{})
  private val optimizer = new Optimizer()

  // could move this to an nrc utility in framework
  private def parseProgram(query: Query, shred: Boolean = false): Program = {
    // make sure we are sending a program (requires at least one <= assignment)
    val qbody = if (!query.body.contains("=>")) s"${query.title} <= ${query.body}" else query.body

    // parse the input query string
    val program = parser.parse(qbody).get.asInstanceOf[Program]

    // shred if necessary
    if (shred){
      val (shredded, shreddedCtx) = shredCtx(program)
      val optShredded = optimize(shredded)
      val materializedProgram = materialize(optShredded, eliminateDomains = true)
      materializedProgram.program
    }else program

  }

  // use the json writer from framework.nrc to write 'er
  private def getJsonProgram(program: Program): String = {
    JsonWriter.produceJsonString(program.asInstanceOf[JsonWriter.Program]).replace("\n", "")
  }

  private def compileProgram(program: Program): CExpr = {
    val ncalc = normalizer.finalize(translate(program)).asInstanceOf[CExpr]
    optimizer.applyAll(Unnester.unnest(ncalc)(Map(), Map(), None, "_2"))
  }

  private def getJsonPlan(plan: CExpr): String = {
    PJsonWriter.produceJsonString(plan)
  }

  private def writeParagraph(appname: String, header: String, gcode: String, encoders: String, shred: Boolean): String  = {
    val data = 
      if (shred){
      s"""
        |val IBag_samples__D = spark.emptyDataset[Biospec]
        |val IBag_occurrences__D = spark.emptyDataset[OccurrDict1]
        |val IDict_occurrences__D_transcript_consequences = spark.emptyDataset[OccurTransDict2Mid]
        |val IDict_occurrences__D_transcript_consequences_consequence_terms = spark.emptyDataset[OccurrTransConseqDict3]
        |val IBag_copynumber__D = spark.emptyDataset[CopyNumber]
      """.stripMargin
    }else{
      s"""
        |val samples = spark.emptyDataset[Biospec]
        |val occurrences = spark.emptyDataset[OccurrenceMid]
        |val copynumber = spark.emptyDataset[CopyNumber]
      """.stripMargin
    }

    s"""
      |/** Generated code **/
      |import org.apache.spark.sql._
      |import org.apache.spark.sql.functions._
      |import org.apache.spark.sql.catalyst.plans.logical._
      |import org.apache.spark.sql.types._
      |import org.apache.spark.sql.expressions.scalalang._
      |import sparkutils._
      |import sparkutils.loader._
      |import sparkutils.skew.SkewDataset._
      |import java.io._
      |spark.sparkContext.addJar(s"/app/sparkutils_2.12-0.1.jar")
      |$header
      |case class Stat(name: String, sizeInBytes:String, rowCount:String)
      |$encoders
      |import spark.implicits._
      |$data
      |$gcode
    """.stripMargin
  }

  private def runProgram(plan: CExpr, queryId: String, shred: Boolean = false): String = {

    val zep = new ZeppelinFactory(host = "localhost", port = 8085)

    val anfBase = new BaseOperatorANF{}
    val anfer = new Finalizer(anfBase)
    val anfed = anfBase.anf(anfer.finalize(plan).asInstanceOf[anfBase.Rep])
    val generator = new SparkDatasetGenerator(false, false, evalFinal=true)
    val code = generator.generate(anfed)

    val noteid = zep.addNote(queryId)
    val pcontents = writeParagraph(queryId, generator.generateHeader(), code, generator.generateEncoders(), shred)
    val para = new ZJsonWriter().buildParagraph("Generated paragraph test", pcontents)
    val pid = zep.writeParagraph(noteid, para)
     val status = zep.runParaSync(noteid, pid)
    pid

  }

  @ApiOperation(
    value = "Find all Querys",
    response = classOf[Query],
    responseContainer = "List"
  )
  def getAll=Action.async {
    queryRepository.getAll.map{ query =>
      Ok(Json.toJson(query))
    }
  }

  @ApiOperation(
    value = "Get a Query",
    response = classOf[TranceObject]
  )
  @ApiResponses(Array(
    new ApiResponse(code = 404, message = "Query not found")
  ))
  def getQuery(@ApiParam(value="The id of the Query to fetch") id: UUID) =
    Action.async{ req =>
      queryRepository.getSingleItem(id).map{ maybeQuery =>
        maybeQuery.map { query =>
          Ok(Json.toJson(query))
        }.getOrElse(NotFound)
      }}


  @ApiOperation(
    value = "Add a Query to the list",
    response = classOf[Void],
    code=201
  )
  @ApiResponses(Array(
    new ApiResponse(code = 400, message = "Invalid Query format")
  ))
  @ApiImplicitParams(Array(
    new ApiImplicitParam(value = "The new Query in Json Format", required = true, dataType = "models.Query", paramType = "body")
  ))
  def createQuery() =
    Action.async(parse.json) {

    _.body.validate[Query].map { query =>

        val program = parseProgram(query)
        val nrc = getJsonProgram(program)

        // note that i'm sending back the nrc and the standard plan
        // also note that the JsonWriter in framework.plans is not complete,
        // so will need to do that
        val responseBody = s"""{"nrc": $nrc}"""

        queryRepository.addEntity(query).map{ _ =>
          Created(responseBody)
        }

    // note that my parser does not return any valuable information
    // so we will need some better error catching there
    }.getOrElse(Future.successful(BadRequest("Invalid nrc format")))
  }

  @ApiOperation(
    value = "Execute query",
    response = classOf[Void],
    code=201
  )
  @ApiResponses(Array(
    new ApiResponse(code = 400, message = "Invalid Query format")
  ))
  @ApiImplicitParams(Array(
    new ApiImplicitParam(value = "The id of the query", required = true, dataType = "models.Query", paramType = "body")
  ))
  def executeQuery() =
    Action.async(parse.json) {

    _.body.validate[Query].map { query =>

        val qid = query._id match { 
          case Some(id) => id.toString
          case _ => sys.error("Id not found.")
        }

        val isShred = query.shred match {
          case Some(b) => b
          case _ => false
        }

        val name = s"${query.title}_${qid}"

        val program = parseProgram(query)
        val nrc = getJsonProgram(program)

        val plan = compileProgram(program)

        val runstatus = runProgram(plan, name, shred = isShred)

        val responseBody = s"""{"status": $plan}"""

        queryRepository.addEntity(query).map{ _ =>
          Created(responseBody)
        }

    // note that my parser does not return any valuable information
    // so we will need some better error catching there
    }.getOrElse(Future.successful(BadRequest("Invalid nrc format")))
  }

  @ApiOperation(
    value = "Add a Query to the list",
    response = classOf[Void],
    code=201
  )
  @ApiResponses(Array(
    new ApiResponse(code = 400, message = "Invalid Query format")
  ))
  @ApiImplicitParams(Array(
    new ApiImplicitParam(value = "The new Query in Json Format", required = true, dataType = "models.Query", paramType = "body")
  ))
  def standardQuery() =
    Action.async(parse.json) {

    _.body.validate[Query].map { query =>

        val program = parseProgram(query)
        
        val plan = compileProgram(program)
        val standard_plan = getJsonPlan(plan)

        val responseBody = s"""{"standard_plan": $standard_plan}"""

        queryRepository.addEntity(query).map{ _ =>
          Created(responseBody)
        }

    }.getOrElse(Future.successful(BadRequest("Invalid nrc format")))
  }

  @ApiOperation(
    value = "Add a Query to the list",
    response = classOf[Void],
    code=201
  )
  @ApiResponses(Array(
    new ApiResponse(code = 400, message = "Invalid Query format")
  ))
  @ApiImplicitParams(Array(
    new ApiImplicitParam(value = "The new Query in Json Format", required = true, dataType = "models.Query", paramType = "body")
  ))
  def shredQuery() =
    Action.async(parse.json) {

    _.body.validate[Query].map { query =>

        val program = parseProgram(query, shred = true)
        val shred_nrc = getJsonProgram(program)

        val plan = compileProgram(program)
        val shred_plan = getJsonPlan(plan)

        val responseBody = s"""{"shred_plan": $shred_plan, "shred_nrc": $shred_nrc}"""

        queryRepository.addEntity(query).map{ _ =>
          Created(responseBody)
        }

    }.getOrElse(Future.successful(BadRequest("Invalid nrc format")))
  }

  @ApiOperation(
    value = "Update a Query",
    response = classOf[Query]
  )
  @ApiResponses(Array(
    new ApiResponse(code = 400, message = "Invalid Query format")
  )
  )
  @ApiImplicitParams(Array(
    new ApiImplicitParam(value = "The updated Query, in Json Format", required = true, dataType = "models.Query", paramType = "body")
  )
  )
  def updateUpdateQuery(@ApiParam(value = "The id of the Query to update")id: UUID) =
    Action.async(parse.json) {
      req => req.body.validate[Query].map { query =>
        queryRepository.updateEntity(id, query).map {
          case Some(query) => Ok(Json.toJson(query))
          case _ => NotFound
        }
      }.getOrElse(Future.successful(BadRequest("Invalid Json")))
    }

  @ApiOperation(
    value = "Delete a Query",
    response = classOf[Query]
  )
  def delete(@ApiParam(value = "The id of the Query to delete") id: UUID) = Action.async { req =>
    queryRepository.deleteEntity(id).map {
      case Some(query) => Ok(Json.toJson(query))
      case _ => NotFound
    }
  }

}
