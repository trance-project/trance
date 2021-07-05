package framework.examples.genomic

import framework.common._
import framework.examples.Query
import framework.nrc.Parser

class LetTest0(override val letOpt: Boolean = false) extends DriverGene {

  override def loadTables(shred: Boolean = false, skew: Boolean = false): String =
    if (shred){
      s"""|val samples = spark.table("samples")
          |val IBag_samples__D = samples
          |
          |val copynumber = spark.table("copynumber")
          |val IBag_copynumber__D = copynumber
          |
          |val odict1 = spark.table("odict1")
          |val IBag_occurrences__D = odict1
          |
          |// issue with partial shredding here
          |val odict2 = spark.table("odict2").drop("flags")
          |val IMap_occurrences__D_transcript_consequences = odict2
          |
          |val odict3 = spark.table("odict3")
          |val IMap_occurrences__D_transcript_consequences_consequence_terms = odict3
          |""".stripMargin
    }else{
      s"""|val samples = spark.table("samples")
          |
          |val copynumber = spark.table("copynumber")
          |
          |val occurrences = spark.table("occurrences")
          |""".stripMargin
    }

  val name = "LetTest0"
  
  val tbls = Map("occurrences" -> occurmids.tp, 
                  "copynumber" -> copynum.tp, 
                  "samples" -> samples.tp)

    val imp = """if (t.impact = "HIGH") then 0.80 
                  else if (t.impact = "MODERATE") then 0.50
                  else if (t.impact = "LOW") then 0.30
                  else 0.01"""

    // val query = 
    // 	s"""
    //     initScores <= 
    //    let cnvCases := 
	  //       for s in samples union 
	  //         for c in copynumber union 
	  //           if (s.bcr_aliquot_uuid = c.cn_aliquot_uuid)
	  //           then {(sid := s.bcr_patient_uuid, gene := c.cn_gene_id, cnum := c.cn_copy_number)}
	  //   in 
    // 			(for o in occurrences union 
    // 				for t in o.transcript_consequences union 
    // 					for c in cnvCases union 
    // 						if (o.donorId = c.sid && t.gene_id = c.gene)
    // 						then {(hybrid_case := o.donorId, hybrid_gene := t.gene_id, hybrid_score := $imp * (c.cnum + 0.01))}).sumBy({hybrid_case, hybrid_gene}, {hybrid_score});
    	
    //     LetTest0 <= 
    // 		for s in samples union 
    // 			{( hybrid_sample := s.bcr_patient_uuid,
    // 			   hybrid_aliquot := s.bcr_aliquot_uuid,
    // 			   hybrid_center := s.center_id,
    // 			   hybrid_genes := for i in initScores union
    // 			   	if (s.bcr_patient_uuid = i.hybrid_case)
    // 			   	then {(hybrid_gene := i.hybrid_gene, hybrid_score := i.hybrid_gene)})}
    // 	"""

    val cnvs = 
      s"""
      for s in samples union 
        for c1 in copynumber union 
          if (s.bcr_aliquot_uuid = c1.cn_aliquot_uuid)
          then {(sid := s.bcr_patient_uuid, gene := c1.cn_gene_id, cnum := c1.cn_copy_number)}
      """
    val initScores = 
      s"""
         (for o in occurrences union 
           for t in o.transcript_consequences union 
             for c in cnvCases union 
               if (o.donorId = c.sid && t.gene_id = c.gene)
               then {(hybrid_case := o.donorId, hybrid_gene := t.gene_id, hybrid_score := (c.cnum + 0.01) * $imp )}).sumBy({hybrid_case, hybrid_gene}, {hybrid_score})
      """

      val lettest = 
        s"""
         for s in samples union 
           {( hybrid_sample := s.bcr_patient_uuid,
              hybrid_aliquot := s.bcr_aliquot_uuid,
              hybrid_center := s.center_id,
              hybrid_genes := for i in $initScores union
               if (s.bcr_patient_uuid = i.hybrid_case)
               then {(hybrid_gene := i.hybrid_gene, hybrid_score := i.hybrid_score)})}
       """

    val query =
      s"""
        LetTest0 <= 
          for o in occurrences union 
            {(sid := o.donorId, cons := 
              for t in o.transcript_consequences union 
                {(gene := t.gene_id)}
            )}
      """

      // s"""
      //   LetTest0 <= 
      //     let cnvCases := $cnvs
      //     in let initScores0 := $initScores 
      //     in $lettest
      // """

    val parser = Parser(tbls)
    val program = parser.parse(query).get.asInstanceOf[Program]
    // val bagexpr = parser.parse(query, parser.term).get.asInstanceOf[BagExpr]
    // val program = Program(Assignment(name, bagexpr))

}

object LetTest1 extends DriverGene {

  override def loadTables(shred: Boolean = false, skew: Boolean = false): String =
    if (shred){
      s"""|val samples = spark.table("samples")
          |val IBag_samples__D = samples
          |
          |val copynumber = spark.table("copynumber")
          |val IBag_copynumber__D = copynumber
          |
          |val odict1 = spark.table("odict1")
          |val IBag_occurrences__D = odict1
          |
          |// issue with partial shredding here
          |val odict2 = spark.table("odict2").drop("flags")
          |val IMap_occurrences__D_transcript_consequences = odict2
          |
          |val odict3 = spark.table("odict3")
          |val IMap_occurrences__D_transcript_consequences_consequence_terms = odict3
          |""".stripMargin
    }else{
      s"""|val samples = spark.table("samples")
          |
          |val copynumber = spark.table("copynumber")
          |
          |val occurrences = spark.table("occurrences")
          |""".stripMargin
    }

  val name = "LetTest1"
  
  val tbls = Map("occurrences" -> occurmids.tp, 
                  "copynumber" -> copynum.tp, 
                  "samples" -> samples.tp)

    val imp = """if (t.impact = "HIGH") then 0.80 
                  else if (t.impact = "MODERATE") then 0.50
                  else if (t.impact = "LOW") then 0.30
                  else 0.01"""

    val query = 
     s"""
        cnvCases <=
            for s in samples union 
              for c in copynumber union 
                if (s.bcr_aliquot_uuid = c.cn_aliquot_uuid)
                then {(sid := s.bcr_patient_uuid, gene := c.cn_gene_id, cnum := c.cn_copy_number)};

        initScores1 <=  
         (for o in occurrences union 
           for t in o.transcript_consequences union 
             for c in cnvCases union 
               if (o.donorId = c.sid && t.gene_id = c.gene)
               then {(hybrid_case := o.donorId, hybrid_gene := t.gene_id, hybrid_score := (c.cnum + 0.01) * $imp )}).sumBy({hybrid_case, hybrid_gene}, {hybrid_score});
      
        LetTest1 <= 
         for s in samples union 
           {( hybrid_sample := s.bcr_patient_uuid,
              hybrid_aliquot := s.bcr_aliquot_uuid,
              hybrid_center := s.center_id,
              hybrid_genes := for i in initScores1 union
               if (s.bcr_patient_uuid = i.hybrid_case)
               then {(hybrid_gene := i.hybrid_gene, hybrid_score := i.hybrid_score)})}
     """

    val parser = Parser(tbls)
    val program = parser.parse(query).get.asInstanceOf[Program]
    // val bagexpr = parser.parse(query, parser.term).get.asInstanceOf[BagExpr]
    // val program = Program(Assignment(name, bagexpr))

}

object LetTest2 extends DriverGene {

  override def loadTables(shred: Boolean = false, skew: Boolean = false): String =
    if (shred){
      s"""|val samples = spark.table("samples")
          |val IBag_samples__D = samples
          |
          |val copynumber = spark.table("copynumber")
          |val IBag_copynumber__D = copynumber
          |
          |val odict1 = spark.table("odict1")
          |val IBag_occurrences__D = odict1
          |
          |// issue with partial shredding here
          |val odict2 = spark.table("odict2").drop("flags")
          |val IMap_occurrences__D_transcript_consequences = odict2
          |
          |val odict3 = spark.table("odict3")
          |val IMap_occurrences__D_transcript_consequences_consequence_terms = odict3
          |""".stripMargin
    }else{
      s"""|val samples = spark.table("samples")
          |
          |val copynumber = spark.table("copynumber")
          |
          |val occurrences = spark.table("occurrences")
          |""".stripMargin
    }

  val name = "LetTest2"
  
  val tbls = Map("occurrences" -> occurmids.tp, 
                  "copynumber" -> copynum.tp, 
                  "samples" -> samples.tp)

    val imp = """if (t.impact = "HIGH") then 0.80 
                  else if (t.impact = "MODERATE") then 0.50
                  else if (t.impact = "LOW") then 0.30
                  else 0.01"""

    val cnvs = 
      s"""
      for s in samples union 
        for c1 in copynumber union 
          if (s.bcr_aliquot_uuid = c1.cn_aliquot_uuid)
          then {(sid := s.bcr_patient_uuid, gene := c1.cn_gene_id, cnum := c1.cn_copy_number)}
      """
    val initScores = 
      s"""
         (for o in occurrences union 
           for t in o.transcript_consequences union 
             for c in $cnvs union 
               if (o.donorId = c.sid && t.gene_id = c.gene)
               then {(hybrid_case := o.donorId, hybrid_gene := t.gene_id, hybrid_score := (c.cnum + 0.01) * $imp )}).sumBy({hybrid_case, hybrid_gene}, {hybrid_score})
      """
    val query = 
     s"""
      LetTest2 <= 
       for s in samples union 
         {( hybrid_sample := s.bcr_patient_uuid,
            hybrid_aliquot := s.bcr_aliquot_uuid,
            hybrid_center := s.center_id,
            hybrid_genes := for i in $initScores union
             if (s.bcr_patient_uuid = i.hybrid_case)
             then {(hybrid_gene := i.hybrid_gene, hybrid_score := i.hybrid_score)}
         )}
     """

    val parser = Parser(tbls)
    val program = parser.parse(query).get.asInstanceOf[Program]
}

object LetTest3 extends DriverGene {

  override def loadTables(shred: Boolean = false, skew: Boolean = false): String =
    if (shred){
      s"""|val samples = spark.table("samples")
          |val IBag_samples__D = samples
          |
          |val copynumber = spark.table("copynumber")
          |val IBag_copynumber__D = copynumber
          |
          |val odict1 = spark.table("odict1")
          |val IBag_occurrences__D = odict1
          |
          |// issue with partial shredding here
          |val odict2 = spark.table("odict2").drop("flags")
          |val IMap_occurrences__D_transcript_consequences = odict2
          |
          |val odict3 = spark.table("odict3")
          |val IMap_occurrences__D_transcript_consequences_consequence_terms = odict3
          |""".stripMargin
    }else{
      s"""|val samples = spark.table("samples")
          |
          |val copynumber = spark.table("copynumber")
          |
          |val occurrences = spark.table("occurrences")
          |""".stripMargin
    }

  val name = "LetTest3"
  
  val tbls = Map("occurrences" -> occurmids.tp, 
                  "copynumber" -> copynum.tp, 
                  "samples" -> samples.tp,
                  "network" -> network.tp, 
                  "genemap" -> gpmap.tp)

    val imp = """if (t.impact = "HIGH") then 0.80 
                  else if (t.impact = "MODERATE") then 0.50
                  else if (t.impact = "LOW") then 0.30
                  else 0.01"""

        // flatNetwork1 <= 
        //   for n in network union 
        //     for e in n.edges union 
        //       for g in genemap union 
        //         if (e.edge_protein = g.protein_stable_id)
        //         then {(network_node := n.node_protein, network_edge := g.gene_stable_id, network_combined := e.combined_score)};

        // sampleNetwork <= 
        //   for h in hybridScores1 union 
        //     {(network_sample := h.hybrid_sample, 
        //       network_aliquot := h.hybrid_aliquot, 
        //       network_genes := (for g in h.hybrid_genes union 
        //         for s in flatNetwork1 union 
        //           if (s.network_edge = g.hybrid_gene)
        //           then {(network_protein_id := s.network_node, distance := s.network_combined * g.hybrid_score )}).sumBy({network_protein_id}, {distance})
        //     )}
    val query = 
     s"""
        cnvCases <=
            for s in samples union 
              for c in copynumber union 
                if (s.bcr_aliquot_uuid = c.cn_aliquot_uuid)
                then {(sid := s.bcr_patient_uuid, gene := c.cn_gene_id, cnum := c.cn_copy_number)};
        
        initScores1 <=  
         (for o in occurrences union 
           for t in o.transcript_consequences union 
             for c in cnvCases union 
               if (o.donorId = c.sid && t.gene_id = c.gene)
               then {(hybrid_case := o.donorId, hybrid_gene := t.gene_id, hybrid_score := (c.cnum + 0.01) * $imp )}).sumBy({hybrid_case, hybrid_gene}, {hybrid_score});
        
        intermMatrix <= 
         for s in samples union 
           {( hybrid_sample := s.bcr_patient_uuid,
              hybrid_aliquot := s.bcr_aliquot_uuid,
              hybrid_center := s.center_id,
              hybrid_genes := for i in initScores1 union
               if (s.bcr_patient_uuid = i.hybrid_case)
               then {(hybrid_gene := i.hybrid_gene, hybrid_score := i.hybrid_score)})}
    """
     //    finalMatrix <= 
     //      (for s in intermMatrix union 
     //        for h in intermMatrix.hybrid_genes union 
     //          {(hybrid_gene := h.hybrid_gene, hybrid_score := h.hybrid_score)}).sumBy({hybrid_gene}, {hybrid_score})

     // """

    val parser = Parser(tbls)
    val program = parser.parse(query).get.asInstanceOf[Program]
    // val bagexpr = parser.parse(query, parser.term).get.asInstanceOf[BagExpr]
    // val program = Program(Assignment(name, bagexpr))

}

object LetTest4 extends DriverGene {

  override def loadTables(shred: Boolean = false, skew: Boolean = false): String =
    if (shred){
      s"""|val samples = spark.table("samples")
          |val IBag_samples__D = samples
          |
          |val copynumber = spark.table("copynumber")
          |val IBag_copynumber__D = copynumber
          |
          |val odict1 = spark.table("odict1")
          |val IBag_occurrences__D = odict1
          |
          |// issue with partial shredding here
          |val odict2 = spark.table("odict2").drop("flags")
          |val IMap_occurrences__D_transcript_consequences = odict2
          |
          |val odict3 = spark.table("odict3")
          |val IMap_occurrences__D_transcript_consequences_consequence_terms = odict3
          |""".stripMargin
    }else{
      s"""|val samples = spark.table("samples")
          |
          |val copynumber = spark.table("copynumber")
          |
          |val occurrences = spark.table("occurrences")
          |""".stripMargin
    }

  val name = "LetTest4"
  
  val tbls = Map("occurrences" -> occurmids.tp, 
                  "copynumber" -> copynum.tp, 
                  "samples" -> samples.tp,
                  "network" -> network.tp, 
                  "genemap" -> gpmap.tp)

    val imp = """if (t.impact = "HIGH") then 0.80 
                  else if (t.impact = "MODERATE") then 0.50
                  else if (t.impact = "LOW") then 0.30
                  else 0.01"""

    val cnvs = 
      s"""
      for s in samples union 
        for c1 in copynumber union 
          if (s.bcr_aliquot_uuid = c1.cn_aliquot_uuid)
          then {(sid := s.bcr_patient_uuid, gene := c1.cn_gene_id, cnum := c1.cn_copy_number)}
      """
    val initScores = 
      s"""
        (for o in occurrences union 
          for t in o.transcript_consequences union 
            for c in $cnvs union 
              if (o.donorId = c.sid && t.gene_id = c.gene)
              then {(hybrid_case := o.donorId, hybrid_gene := t.gene_id, hybrid_score := (c.cnum + 0.01) * $imp )}).sumBy({hybrid_case, hybrid_gene}, {hybrid_score})
      """
    val hybridScores = 
     s"""
       for s in samples union 
         {( hybrid_sample := s.bcr_patient_uuid,
            hybrid_aliquot := s.bcr_aliquot_uuid,
            hybrid_genes := for i in $initScores union
              if (s.bcr_patient_uuid = i.hybrid_case)
              then {( hybrid_gene := t.hybrid_gene, hybrid_score := t.hybrid_score )}
          )}
     """

    val flatNetwork = 
      s"""
        for n in network union 
          for e in n.edges union 
            for g in genemap union 
              if (e.edge_protein = g.protein_stable_id)
              then {(network_node := n.node_protein, network_edge := g.gene_stable_id, network_combined := e.combined_score)}
      """

    val query = 
      s"""
        sampleNetwork <= 
          for h in $hybridScores union 
            {(network_sample := h.hybrid_sample, 
              network_aliquot := h.hybrid_aliquot, 
              network_genes := (for g in h.hybrid_genes union 
                for s in $flatNetwork union 
                  if (s.network_edge = g.hybrid_gene)
                  then {(network_protein_id := s.network_node, distance := s.network_combined * g.hybrid_score )}
              ).sumBy({network_protein_id}, {distance})
            )}
      """

    val parser = Parser(tbls)
    val program = parser.parse(query).get.asInstanceOf[Program]
}

object LetTest5 extends DriverGene {

  override def loadTables(shred: Boolean = false, skew: Boolean = false): String =
    if (shred){
      s"""|val samples = spark.table("samples")
          |val IBag_samples__D = samples
          |
          |val copynumber = spark.table("copynumber")
          |val IBag_copynumber__D = copynumber
          |
          |val odict1 = spark.table("odict1")
          |val IBag_occurrences__D = odict1
          |
          |// issue with partial shredding here
          |val odict2 = spark.table("odict2").drop("flags")
          |val IMap_occurrences__D_transcript_consequences = odict2
          |
          |val odict3 = spark.table("odict3")
          |val IMap_occurrences__D_transcript_consequences_consequence_terms = odict3
          |""".stripMargin
    }else{
      s"""|val samples = spark.table("samples")
          |
          |val copynumber = spark.table("copynumber")
          |
          |val occurrences = spark.table("occurrences")
          |""".stripMargin
    }

  val name = "LetTest5"
  
  val tbls = Map("occurrences" -> occurmids.tp, 
                  "copynumber" -> copynum.tp, 
                  "samples" -> samples.tp,
                  "network" -> network.tp, 
                  "genemap" -> gpmap.tp)

    val imp = """if (t.impact = "HIGH") then 0.80 
                  else if (t.impact = "MODERATE") then 0.50
                  else if (t.impact = "LOW") then 0.30
                  else 0.01"""

    val cnvs = 
      s"""
      for s in samples union 
        for c1 in copynumber union 
          if (s.bcr_aliquot_uuid = c1.cn_aliquot_uuid)
          then {(sid := s.bcr_patient_uuid, gene := c1.cn_gene_id, cnum := c1.cn_copy_number)}
      """
    val initScores = 
      s"""
        for o in occurrences union 
          {(hybrid_case := o.donorId, transcript_consequences := o.donorId, hybrid_scores := 
              (for t in o.transcript_consequences union 
                for c in $cnvs union 
                  if (o.donorId = c.sid && t.gene_id = c.gene)
                  then {(x := t.gene_id, hybrid_gene := t.gene_id, hybrid_score := (c.cnum + 0.01) * $imp )}).sumBy({x, hybrid_gene}, {hybrid_score})
          )}
      """
    val query = 
     s"""
       LetTest5 <= 
       for i in $initScores union
         for h in i.hybrid_scores union 
           if (i.transcript_consequences = h.x)
           then {(hybrid_gene := h.hybrid_gene, hybrid_score := h.hybrid_score)}
     """
    val query2 =
     s"""
       LetTest5 <= 
       let initScores := $initScores 
       in for i in initScores union
         for h in i.hybrid_scores union 
           {(hybrid_gene := h.hybrid_gene, hybrid_score := h.hybrid_score)}
     """
    val f2fquery =
    s"""
       Flat2FlatTest <=
           let samplesCopy :=
             for s in samples union
             {( sid := s.bcr_patient_uuid,
                bag := for c1 in copynumber union
                         if (s.bcr_aliquot_uuid = c1.cn_aliquot_uuid)
                         then {(gene := c1.cn_gene_id, cnum := c1.cn_copy_number)} )}
           in for x in samplesCopy union
                {( sid := x.sid )}""".stripMargin
    val f2fquery2 =
    s"""
       Flat2FlatTest2 <=
           let samplesCopy :=
             for s in samples union
             {( sid := s.bcr_patient_uuid,
                bag := for c1 in copynumber union
                         if (s.bcr_aliquot_uuid = c1.cn_aliquot_uuid)
                         then {(gene := c1.cn_gene_id, cnum := c1.cn_copy_number)} )}
           in for x in samplesCopy union
                for y in x.bag union
                  {( sid := x.sid, gene := y.gene )}""".stripMargin

    val parser = Parser(tbls)
    val program = parser.parse(query).get.asInstanceOf[Program]
    val program2 = parser.parse(query2).get.asInstanceOf[Program]
    val f2fprogram = parser.parse(f2fquery).get.asInstanceOf[Program]
    val f2fprogram2 = parser.parse(f2fquery2).get.asInstanceOf[Program]
}

object LetTest5Seq extends DriverGene {

  override def loadTables(shred: Boolean = false, skew: Boolean = false): String =
    if (shred){
      s"""|val samples = spark.table("samples")
          |val IBag_samples__D = samples
          |
          |val copynumber = spark.table("copynumber")
          |val IBag_copynumber__D = copynumber
          |
          |val odict1 = spark.table("odict1")
          |val IBag_occurrences__D = odict1
          |
          |// issue with partial shredding here
          |val odict2 = spark.table("odict2").drop("flags")
          |val IMap_occurrences__D_transcript_consequences = odict2
          |
          |val odict3 = spark.table("odict3")
          |val IMap_occurrences__D_transcript_consequences_consequence_terms = odict3
          |""".stripMargin
    }else{
      s"""|val samples = spark.table("samples")
          |
          |val copynumber = spark.table("copynumber")
          |
          |val occurrences = spark.table("occurrences")
          |""".stripMargin
    }

  val name = "LetTest5Seq"
  
  val tbls = Map("occurrences" -> occurmids.tp, 
                  "copynumber" -> copynum.tp, 
                  "samples" -> samples.tp,
                  "network" -> network.tp, 
                  "genemap" -> gpmap.tp)

    val imp = """if (t.impact = "HIGH") then 0.80 
                  else if (t.impact = "MODERATE") then 0.50
                  else if (t.impact = "LOW") then 0.30
                  else 0.01"""

    val cnvs = 
      s"""
      for s in samples union 
        for c1 in copynumber union 
          if (s.bcr_aliquot_uuid = c1.cn_aliquot_uuid)
          then {(sid := s.bcr_patient_uuid, gene := c1.cn_gene_id, cnum := c1.cn_copy_number)}
      """
    val initScores = 
      s"""
        for o in occurrences union 
          {(hybrid_case := o.donorId, hybrid_scores := 
              (for t in o.transcript_consequences union 
                for c in $cnvs union 
                  if (o.donorId = c.sid && t.gene_id = c.gene)
                  then {(hybrid_gene := t.gene_id, hybrid_score := (c.cnum + 0.01) * $imp )}).sumBy({hybrid_gene}, {hybrid_score})
          )}
      """
    val query = 
     s"""
       cnvTmp <= $cnvs;
       
       initScores <= $initScores; 

       LetTest5 <= 
         for i in initScores union
           for h in i.hybrid_scores union 
             {(hybrid_gene := h.hybrid_gene, hybrid_score := h.hybrid_score)}
     """

    val parser = Parser(tbls)
    val program = parser.parse(query).get.asInstanceOf[Program]

}

object LetTest6 extends DriverGene {

  override def loadTables(shred: Boolean = false, skew: Boolean = false): String =
    if (shred){
      s"""|val samples = spark.table("samples")
          |val IBag_samples__D = samples
          |
          |val copynumber = spark.table("copynumber")
          |val IBag_copynumber__D = copynumber
          |
          |val odict1 = spark.table("odict1")
          |val IBag_occurrences__D = odict1
          |
          |// issue with partial shredding here
          |val odict2 = spark.table("odict2").drop("flags")
          |val IMap_occurrences__D_transcript_consequences = odict2
          |
          |val odict3 = spark.table("odict3")
          |val IMap_occurrences__D_transcript_consequences_consequence_terms = odict3
          |""".stripMargin
    }else{
      s"""|val samples = spark.table("samples")
          |
          |val copynumber = spark.table("copynumber")
          |
          |val occurrences = spark.table("occurrences")
          |""".stripMargin
    }

  val name = "LetTest6"
  
  val tbls = Map("occurrences" -> occurmids.tp, 
                  "copynumber" -> copynum.tp, 
                  "samples" -> samples.tp,
                  "network" -> network.tp, 
                  "genemap" -> gpmap.tp)

    val imp = """if (t.impact = "HIGH") then 0.80 
                  else if (t.impact = "MODERATE") then 0.50
                  else if (t.impact = "LOW") then 0.30
                  else 0.01"""

    val cnvs = 
      s"""
      for s in samples union 
        for c1 in copynumber union 
          if (s.bcr_aliquot_uuid = c1.cn_aliquot_uuid)
          then {(sid := s.bcr_patient_uuid, gene := c1.cn_gene_id, cnum := c1.cn_copy_number)}
      """
    val initScores = 
      s"""
        for o in occurrences union 
          {(hybrid_case := o.donorId, hybrid_scores := 
              (for t in o.transcript_consequences union 
                for c in $cnvs union 
                  if (o.donorId = c.sid && t.gene_id = c.gene)
                  then {(hybrid_gene := t.gene_id, hybrid_score := (c.cnum + 0.01) * $imp )}).sumBy({hybrid_gene}, {hybrid_score})
          )}
      """
    val query = 
     s"""
       LetTest6 <= 
       let initScores := $initScores 
       in for s in samples union 
        {(sid := s.bcr_patient_uuid, scores := 
         for i in initScores union
          if (s.bcr_patient_uuid = i.hybrid_case)
          then for h in i.hybrid_scores union 
             {(hybrid_gene := h.hybrid_gene, hybrid_score := h.hybrid_score)})}
     """

    val parser = Parser(tbls)
    val program = parser.parse(query).get.asInstanceOf[Program]
}

object LetTest7 extends DriverGene {

  override def loadTables(shred: Boolean = false, skew: Boolean = false): String =
    if (shred){
      s"""|val samples = spark.table("samples")
          |val IBag_samples__D = samples
          |
          |val copynumber = spark.table("copynumber")
          |val IBag_copynumber__D = copynumber
          |
          |val odict1 = spark.table("odict1")
          |val IBag_occurrences__D = odict1
          |
          |// issue with partial shredding here
          |val odict2 = spark.table("odict2").drop("flags")
          |val IMap_occurrences__D_transcript_consequences = odict2
          |
          |val odict3 = spark.table("odict3")
          |val IMap_occurrences__D_transcript_consequences_consequence_terms = odict3
          |""".stripMargin
    }else{
      s"""|val samples = spark.table("samples")
          |
          |val copynumber = spark.table("copynumber")
          |
          |val occurrences = spark.table("occurrences")
          |""".stripMargin
    }

  val name = "LetTest7a"
  
  val tbls = Map("occurrences" -> occurmids.tp, 
                  "copynumber" -> copynum.tp, 
                  "samples" -> samples.tp,
                  "network" -> network.tp, 
                  "genemap" -> gpmap.tp)

    val agg1 = 
     s"""
      for s in samples union 
        {( bcr_patient_uuid := s.bcr_patient_uuid, cnvs := 
          for c in copynumber union 
            if (s.bcr_aliquot_uuid = c.cn_aliquot_uuid)
            then {( cn_patient_uuid := s.bcr_patient_uuid, cn_gene_id := c.cn_gene_id, cn_copy_number := c.cn_copy_number + 0.001 )}
        )}
     """

    val query = 
      s"""
        LetTest7 <= 
        for t in $agg1 union 
          for x in t.cnvs union 
            if (t.bcr_patient_uuid = x.cn_patient_uuid)
            then {(bcr_patient_uuid := t.bcr_patient_uuid, cn_gene_id := x.cn_gene_id, cn_copy_number := x.cn_copy_number )}

      """

    val parser = Parser(tbls)
    val program = parser.parse(query).get.asInstanceOf[Program]
}

object LetTest7Seq extends DriverGene {

  override def loadTables(shred: Boolean = false, skew: Boolean = false): String =
    if (shred){
      s"""|val samples = spark.table("samples")
          |val IBag_samples__D = samples
          |
          |val copynumber = spark.table("copynumber")
          |val IBag_copynumber__D = copynumber
          |
          |val odict1 = spark.table("odict1")
          |val IBag_occurrences__D = odict1
          |
          |// issue with partial shredding here
          |val odict2 = spark.table("odict2").drop("flags")
          |val IMap_occurrences__D_transcript_consequences = odict2
          |
          |val odict3 = spark.table("odict3")
          |val IMap_occurrences__D_transcript_consequences_consequence_terms = odict3
          |""".stripMargin
    }else{
      s"""|val samples = spark.table("samples")
          |
          |val copynumber = spark.table("copynumber")
          |
          |val occurrences = spark.table("occurrences")
          |""".stripMargin
    }

  val name = "LetTest7aSeq"
  
  val tbls = Map("occurrences" -> occurmids.tp, 
                  "copynumber" -> copynum.tp, 
                  "samples" -> samples.tp,
                  "network" -> network.tp, 
                  "genemap" -> gpmap.tp)

    val agg1 = 
     s"""
      for s in samples union 
        {( bcr_patient_uuid := s.bcr_patient_uuid, cnvs := 
          for c in copynumber union 
            if (s.bcr_aliquot_uuid = c.cn_aliquot_uuid)
            then {( cn_aliquot_uuid := c.cn_aliquot_uuid, cn_gene_id := c.cn_gene_id, cn_copy_number := c.cn_copy_number + 0.001 )}
        )}
     """

    val query = 
      s"""
        Agg1 <= $agg1;

        LetTest7 <= 
        for t in Agg1 union 
          for x in t.cnvs union 
            {(bcr_patient_uuid := t.bcr_patient_uuid, cn_gene_id := x.cn_gene_id, cn_copy_number := x.cn_copy_number  )}

      """

    val parser = Parser(tbls)
    val program = parser.parse(query).get.asInstanceOf[Program]
}

object LetTest0 {
  def apply(letOpt: Boolean = false): Query = new LetTest0(letOpt)
  def apply(): LetTest0 = new LetTest0()
}