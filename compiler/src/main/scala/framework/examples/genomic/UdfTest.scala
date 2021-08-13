package framework.examples.genomic

import java.util.Base64
import framework.common._
import framework.examples.Query
import framework.nrc.Parser
// test2

object MultiOmicsProstate extends DriverGene {
  val sampleFile = "/mnt/app_hdd/data/biospecimen/aliquot/nationwidechildrens.org_biospecimen_aliquot_prad.txt"
  val cnvFile = "/mnt/app_hdd/data/cnv"
  val exprFile = "/mnt/app_hdd/data/expression/"
  val aexprFile = "/mnt/app_hdd/data/fpkm_uq_case_aliquot.txt"
  val occurFile = "/mnt/app_hdd/data/somatic/"
  val occurName = "datasetPRAD"
  val occurDicts = ("odictPrad1", "odictPrad2", "odictPrad3")
  val pathFile = "/mnt/app_hdd/data/pathway/c2.cp.v7.1.symbols.gmt"
  val gtfFile = "/mnt/app_hdd/data/genes/Homo_sapiens.GRCh37.87.chr.gtf"
  val pradFile = "/mnt/app_hdd/data/biospecimen/clinical/nationwidechildrens.org_clinical_patient_prad.txt"
  val clinDir = "/mnt/app_hdd/data/biospecimen/clinical"

  override def loadTables(shred: Boolean = false, skew: Boolean = false): String =
    s"""|${loadBiospec(shred, skew, fname = pradFile, name = "clinical", func = "Prad")}
        |${loadBiospec(shred, skew, fname = sampleFile, name = "samples")}
        |${loadGeneExpr(shred, skew, fname = exprFile, aname = aexprFile)}
        |${loadOccurrence(shred, skew, fname = occurFile, iname = occurName, dictNames = occurDicts)}
        |${loadGtfTable(shred, skew, fname = gtfFile)}
        |""".stripMargin

  // name to identify your query
  val name = "ExampleQueryMultiOmicsProstate"

  // a map of input types for the parser
    val tbls = Map("occurrences" -> occurmids.tp,
                    "copynumber" -> copynum.tp,
                    "expression" -> fexpression.tp,
                    "samples" -> samples.tp,
                    "pathways" -> pathway.tp,
                    //"clinical" -> BagType(tcgaType),// this is for the tcga only
                    "genemap" -> gtf.tp,
                    "clinical" -> BagType(pradType))

  val query =
    s"""
        mapExpression <=
          for s in samples union
            for e in expression union
              if (s.bcr_aliquot_uuid = e.ge_aliquot) then
                {(msid := s.bcr_patient_uuid, gene := e.ge_gene_id, fpkm := e.ge_fpkm)};

         impactGMB <=
          for g in genemap union
            {(gene_name := g.g_gene_name, gene_id:= g.g_gene_id, burdens :=
              (for o in occurrences union
                for s in clinical union
                  if (o.donorId = s.bcr_patient_uuid) then
                    for t in o.transcript_consequences union
                      if (g.g_gene_id = t.gene_id) then
                         {(sid := o.donorId,
                           lbl := if (s.gleason_pattern_primary = 2) then 0
                            else if (s.gleason_pattern_primary = 3) then 0
                            else if (s.gleason_pattern_primary = 4) then 1
                            else if (s.gleason_pattern_primary = 5) then 1
                            else -1,
                           burden := if (t.impact = "HIGH") then 0.80
                                                    else if (t.impact = "MODERATE") then 0.50
                                                    else if (t.impact = "LOW") then 0.30
                                                    else 0.01
                          )}
              ).sumBy({sid, lbl}, {burden})
            )};

        GMB <=
          for g in impactGMB union
            {(gene_name := g.gene_name, gene_id := g.gene_id, burdens :=
              (for b in g.burdens union
                for e in mapExpression union
                  if (b.sid = e.msid && g.gene_id = e.gene) then
                    {(sid := b.sid, lbl := b.lbl, burden := b.burden*e.fpkm)}).sumBy({sid,lbl}, {burden})
                     )}
    """

    val parser = Parser(tbls)
    val program = parser.parse(query).get.asInstanceOf[Program]

}

// this is just the example from the 
object ExampleQuery extends DriverGene {
  
  // TODO: update this with the loading functionality in ExampleQuery2
  val sampleFile = "/mnt/app_hdd/data/biospecimen/aliquot/nationwidechildrens.org_biospecimen_aliquot_prad.txt"
  val cnvFile = "/mnt/app_hdd/data/cnv"
  val occurFile = "/mnt/app_hdd/data/somatic/"
  val occurName = "datasetPRAD"
  val occurDicts = ("odictPrad1", "odictPrad2", "odictPrad3")
  val pathFile = "/mnt/app_hdd/data/c2.cp.v7.1.symbols.gmt"
  val gtfFile = "/nfs_qc4/genomics/Homo_sapiens.GRCh37.87.chr.gtf"
  val pradFile = "/mnt/app_hdd/data/biospecimen/clinical/nationwidechildrens.org_clinical_patient_prad.txt"

  // in DriverGenes.scala you can see traits for several datatypes, these 
  // are inherited from DriverGene trait (around line 549)
  // checkout individuals traits to see what the load functions are doing
  override def loadTables(shred: Boolean = false, skew: Boolean = false): String = 
    s"""|${loadBiospec(shred, skew, fname = pradFile, name = "clinical", func = "Prad")}
        |${loadCopyNumber(shred, skew, fname = cnvFile)}
        |${loadOccurrence(shred, skew, fname = occurFile, iname = occurName, dictNames = occurDicts)}
        |${loadPathway(shred, skew, fname = pathFile)}
        |${loadGtfTable(shred, skew, fname = gtfFile)}
        |""".stripMargin
  
  // name to identify your query
  val name = "ExampleQuery"
  // moved these to DriverGenes.scala Pathway train (line 19)
  // val genetype = TupleType("name" -> StringType)
  // val pathtype = TupleType("p_name" -> StringType, "url" -> StringType, "gene_set" -> BagType(genetype))

  // a map of input types for the parser
    val tbls = Map("occurrences" -> occurmids.tp,
                    "copynumber" -> copynum.tp,
                    "samples" -> samples.tp,
                    "pathways" -> pathway.tp,
                    "genemap" -> gtf.tp, 
                    "clinical" -> BagType(pradType))


  // a query string that is passed to the parser
  // note that a list of assignments should be separated with ";"
  val query = 
    // notes from discussion
    // s"""
    //     // defined some udfs
    //     def pivot(df): ... // todo
    //     def sepDF(df): (X,y) // todo
    //     def chi_square(X,y): ... // todo


    //     GMB <=
    //       for g in genemap union
    //         {(gene:= g.g_gene_name, burdens :=
    //           (for o in occurrences union
    //             for t in o.transcript_consequences union
    //               if (g.g_gene_id = t.gene_id) then
    //                  {(sid := o.donorId, lbl := , burden := if (t.impact = "HIGH") then 0.80
    //                                             else if (t.impact = "MODERATE") then 0.50
    //                                             else if (t.impact = "LOW") then 0.30
    //                                             else 0.01)}).sumBy({sid}, {burden}))}


    //     matrix <= pivot(GMB)
    //     (X,y) <= sepDF(matrix)
    //     selected_genes <= chi_square(X,y) // user could specify a gene set
    //     matrix <= matrix[selected_genes] // subset

    // """

    s"""
        GMB <=
          for g in genemap union
            {(gene:= g.g_gene_name, burdens :=
              (for o in occurrences union
                for s in clinical union 
                  if (o.donorId = s.bcr_patient_uuid) then
                    for t in o.transcript_consequences union
                      if (g.g_gene_id = t.gene_id) then
                         {(sid := o.donorId, 
                           lbl := if (s.gleason_pattern_primary = 2) then 0 
                            else if (s.gleason_pattern_primary = 3) then 0
                            else if (s.gleason_pattern_primary = 4) then 1
                            else if (s.gleason_pattern_primary = 5) then 1
                            else -1, 
                           burden := if (t.impact = "HIGH") then 0.80
                                                    else if (t.impact = "MODERATE") then 0.50
                                                    else if (t.impact = "LOW") then 0.30
                                                    else 0.01
                          )}
              ).sumBy({sid, lbl}, {burden})
            )}

    """
    // finally define the parser, note that it takes the input types
    // map as input and pass the query string to the parser to
    // generate the program.
    val parser = Parser(tbls)
    val program = parser.parse(query).get.asInstanceOf[Program]

}

// this is just the example from the 
object ExampleQuery2 extends DriverGene {

  val sampleFile = "/mnt/app_hdd/data/biospecimen/aliquot/nationwidechildrens.org_biospecimen_aliquot_prad.txt"
  val cnvFile = "/mnt/app_hdd/data/cnv"
  val occurFile = "/mnt/app_hdd/data/somatic/"
  val occurName = "datasetPRAD"
  val occurDicts = ("odictPrad1", "odictPrad2", "odictPrad3")
  val pathFile = "/mnt/app_hdd/data/c2.cp.v7.1.symbols.gmt"
  val gtfFile = "/nfs_qc4/genomics/Homo_sapiens.GRCh37.87.chr.gtf"

  // in DriverGenes.scala you can see traits for several datatypes, these 
  // are inherited from DriverGene trait (around line 549)
  // checkout individuals traits to see what the load functions are doing
  override def loadTables(shred: Boolean = false, skew: Boolean = false): String = 
    s"""|${loadBiospec(shred, skew, fname = sampleFile, name = "samples")}
        |${loadCopyNumber(shred, skew, fname = cnvFile)}
        |${loadOccurrence(shred, skew, fname = occurFile, iname = occurName, dictNames = occurDicts)}
        |${loadPathway(shred, skew, fname = pathFile)}
        |${loadGtfTable(shred, skew, fname = gtfFile)}
        |""".stripMargin
  
  // name to identify your query
  val name = "ExampleQuery2"

  // a map of input types for the parser
    val tbls = Map("occurrences" -> occurmids.tp,
                    "copynumber" -> copynum.tp,
                    "samples" -> samples.tp,
                    "pathways" -> pathway.tp,
                    "genemap" -> gtf.tp)


  // a query string that is passed to the parser
  // note that a list of assignments should be separated with ";"
  val query = 
    s"""
        impactScores <= 
          (for o in occurrences union
            for t in o.transcript_consequences union
              {(gid := t.gene_id, sid := o.donorId, burden := if (t.impact = "HIGH") then 0.80
                                                else if (t.impact = "MODERATE") then 0.50
                                                else if (t.impact = "LOW") then 0.30
                                                else 0.01)}).sumBy({gid, sid}, {burden});
        PMB <=
          for p in pathways union
            {(pathway := p.p_name, burdens :=
              (for g in p.gene_set union 
                for g2 in genemap union 
                  if (g.name = g2.g_gene_name) then 
                    for o in impactScores union 
                      if (g2.g_gene_id = o.gid) then
                        {(sid := o.sid, burden := o.burden)}).sumBy({sid}, {burden}))}

    """

    // finally define the parser, note that it takes the input types
    // map as input and pass the query string to the parser to
    // generate the program.
    val parser = Parser(tbls)
    val program = parser.parse(query).get.asInstanceOf[Program]

}
