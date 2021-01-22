package framework.examples.genomic

import framework.common._
import framework.examples.Query
import framework.nrc.Parser

/** Queries for sharing benchmark with filters **/

object HybridSamplesWithoutTP53 extends DriverGene {
  
  override def loadTables(shred: Boolean = false, skew: Boolean = false): String =
    s"""|${super.loadTables(shred, skew)}
        |${loadOccurrence(shred, skew)}
        |${loadCopyNumber(shred, skew)}""".stripMargin

  val name = "HybridSamplesWithoutTP53"
  
  val tbls = Map("samples" -> biospec.tp,
                 "occurrences" -> occurmids.tp, 
                 "cnvCases" -> cnvCases.tp)

  val sampleFilter = 
    s"""
      dedup(for s in samples union 
        for o in occurrences union
          for t in o.transcript_consequences union
            if (t.gene_id != "TP53") then 
              {( cid := s.bcr_patient_uuid, aid := s.bcr_aliquot_uuid )}) 
    """

    val parser = Parser(tbls)
    val query: BagExpr = parser.parse(sampleFilter, parser.term).get.asInstanceOf[BagExpr]
     
    val program = Program(Assignment("FilterSamples", query)) //Program(Assignment("cnvCases", mapCNV), Assignment(name, query))

}

object HybridTP53 extends DriverGene {
  
  override def loadTables(shred: Boolean = false, skew: Boolean = false): String =
    s"""|${super.loadTables(shred, skew)}
        |${loadOccurrence(shred, skew)}
        |${loadCopyNumber(shred, skew)}""".stripMargin

  val name = "HybridTP53"
  
  val tbls = Map("occurrences" -> occurmids.tp, 
                 "cnvCases" -> cnvCases.tp)

  val qstr = 
    s"""
      for o in occurrences union
        {( oid := o.oid, sid := o.donorId, cands := 
          for t in o.transcript_consequences union
            if (t.gene_id != "TP53") then  
              for c in cnvCases union
                if (o.donorId = c.cn_case_uuid && t.gene_id = c.cn_gene_id) then
                  {( gene := t.gene_id, score := if (t.impact = "HIGH") then 0.80 
                      else if (t.impact = "MODERATE") then 0.50
                      else if (t.impact = "LOW") then 0.30
                      else 0.01 * c.cn_copy_number )} )}
    """

     val parser = Parser(tbls)
     val query: BagExpr = parser.parse(qstr, parser.term).get.asInstanceOf[BagExpr]
     
     val program = Program(Assignment("cnvCases", mapCNV), Assignment(name, query))

}


object HybridBRCA extends DriverGene {
  
  override def loadTables(shred: Boolean = false, skew: Boolean = false): String =
    s"""|${super.loadTables(shred, skew)}
        |${loadOccurrence(shred, skew)}
        |${loadCopyNumber(shred, skew)}""".stripMargin

  val name = "HybridBRCA"

  val tbls = Map("occurrences" -> occurmids.tp, 
                 "cnvCases" -> cnvCases.tp)

  val qstr = 
    s"""
      for o in occurrences union
        {( oid := o.oid, sid := o.donorId, cands := 
          for t in o.transcript_consequences union 
            if (t.gene_id != "BRCA") then 
              for c in cnvCases union 
                if ((o.donorId = c.cn_case_uuid) && (t.gene_id = c.cn_gene_id)) then
                  {( gene := t.gene_id, score := if (t.impact = "HIGH") then 0.80 
                      else if (t.impact = "MODERATE") then 0.50
                      else if (t.impact = "LOW") then 0.30
                      else 0.01 * c.cn_copy_number )} )}
     """

     val parser = Parser(tbls)
     val query: BagExpr = parser.parse(qstr, parser.term).get.asInstanceOf[BagExpr]
     
     val program = Program(Assignment("cnvCases", mapCNV), Assignment(name, query))

}


/** Sharing with projection **/

object HybridImpact extends DriverGene {
  
  override def loadTables(shred: Boolean = false, skew: Boolean = false): String =
    s"""|${super.loadTables(shred, skew)}
        |${loadOccurrence(shred, skew)}
        |${loadCopyNumber(shred, skew)}""".stripMargin

  val name = "HybridImpact"
  
  val tbls = Map("occurrences" -> occurmids.tp, 
                 "cnvCases" -> cnvCases.tp)

  val qstr = 
    s"""
      for o in occurrences union
        {( oid := o.oid, sid := o.donorId, cands := 
          for t in o.transcript_consequences union 
            for c in cnvCases union 
              if ((o.donorId = c.cn_case_uuid) && (t.gene_id = c.cn_gene_id)) then
                {( gene := t.gene_id, score := t.impact * (c.cn_copy_num + 0.01))} )}
    """

     val parser = Parser(tbls)
     val query: BagExpr = parser.parse(qstr, parser.term).get.asInstanceOf[BagExpr]
     
     val program = Program(Assignment("cnvCases", mapCNV), Assignment(name, query))

}

object HybridScores extends DriverGene {
  
  override def loadTables(shred: Boolean = false, skew: Boolean = false): String =
    s"""|${super.loadTables(shred, skew)}
        |${loadOccurrence(shred, skew)}
        |${loadCopyNumber(shred, skew)}""".stripMargin

  val name = "HybridScores"

  val tbls = Map("occurrences" -> occurmids.tp, 
                 "cnvCases" -> cnvCases.tp)

  val qstr = 
    s"""
      for o in occurrences union
        {( oid := o.oid, sid := o.donorId, cands := 
          for t in o.transcript_consequences union 
            for c in cnvCases union 
              if ((o.donorId = c.cn_case_uuid) && (t.gene_id = c.cn_gene_id)) then
                {( gene := t.gene_id, score := t.polyphen_score * t.sift_score * (c.cn_copy_num + 0.01))} )}
     """

     val parser = Parser(tbls)
     val query: BagExpr = parser.parse(qstr, parser.term).get.asInstanceOf[BagExpr]
     
     val program = Program(Assignment("cnvCases", mapCNV), Assignment(name, query))

}
