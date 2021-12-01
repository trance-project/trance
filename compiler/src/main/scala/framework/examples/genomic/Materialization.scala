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
          |IBag_samples__D.cache; IBag_samples__D.count
          |val copynumber = spark.table("copynumber")
          |val IBag_copynumber__D = copynumber
          |IBag_copynumber__D.cache; IBag_copynumber__D.count
          |val odict1 = spark.table("fodict1")
          |val IBag_occurrences__D = odict1
          |IBag_occurrences__D.cache; IBag_occurrences__D.count
          |// issue with partial shredding here
          |val odict2 = spark.table("fodict2").drop("flags")
          |val IMap_occurrences__D_transcript_consequences = odict2
          |IMap_occurrences__D_transcript_consequences.cache; IMap_occurrences__D_transcript_consequences.count
          |val odict3 = spark.table("fodict3")
          |val IMap_occurrences__D_transcript_consequences_consequence_terms = odict3
    		  |IMap_occurrences__D_transcript_consequences_consequence_terms.cache
    		  |IMap_occurrences__D_transcript_consequences_consequence_terms.count
          |""".stripMargin
    }else{
      s"""|val samples = spark.table("samples")
          |
          |val copynumber = spark.table("copynumber")
          |
          |val occurrences = spark.table("foccurrences")
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

object LetTest5F extends DriverGene {

  override def loadTables(shred: Boolean = false, skew: Boolean = false): String =
    if (shred){
      s"""|val samples = spark.table("samples")
          |val IBag_samples__D = samples
          |IBag_samples__D.cache; IBag_samples__D.count
          |val copynumber = spark.table("copynumber")
          |val IBag_copynumber__D = copynumber
          |IBag_copynumber__D.cache; IBag_copynumber__D.count
          |val odict1 = spark.table("fodict1")
          |val IBag_occurrences__D = odict1
          |IBag_occurrences__D.cache; IBag_occurrences__D.count
          |// issue with partial shredding here
          |val odict2 = spark.table("fodict2").drop("flags")
          |val IMap_occurrences__D_transcript_consequences = odict2
          |IMap_occurrences__D_transcript_consequences.cache; IMap_occurrences__D_transcript_consequences.count
          |val odict3 = spark.table("fodict3")
          |val IMap_occurrences__D_transcript_consequences_consequence_terms = odict3
          |IMap_occurrences__D_transcript_consequences_consequence_terms.cache
          |IMap_occurrences__D_transcript_consequences_consequence_terms.count
          |""".stripMargin
    }else{
      s"""|val samples = spark.table("samples")
          |
          |val copynumber = spark.table("copynumber")
          |
          |val occurrences = spark.table("foccurrences")
          |""".stripMargin
    }

  val name = "LetTest5F"
  
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

    // this saves the top level mappings...
    val toccur = 
      s"""
        for o in occurrences union 
          {( donorId := o.donorId, oid := o.oid )}
      """

    val foccur = 
      s"""
        for o in occurrences union 
          for t in o.transcript_consequences union 
            {( fdonorId := o.donorId, foid := o.oid, fgeneid := t.gene_id, fimp := $imp )}
      """

    val initScores = 
      s"""
        for o in TOccur union 
          {(hybrid_case := o.donorId, hybrid_scores := 
              (for t in FOccur union 
                if (o.oid = t.foid && o.donorId = t.fdonorId)
                then for c in $cnvs union 
                  if (o.donorId = c.sid && t.fgeneid = c.gene)
                  then {(hybrid_gene := t.fgeneid, hybrid_score := (c.cnum + 0.01) * t.fimp )}).sumBy({hybrid_gene}, {hybrid_score})
          )}
      """

    val query = 
     s"""
       TOccur <= $toccur; 

       FOccur <= $foccur;

       LetTest5 <= 
       for i in $initScores union
         for h in i.hybrid_scores union 
           {(hybrid_gene := h.hybrid_gene, hybrid_score := h.hybrid_score)}
     """

    val parser = Parser(tbls)
    val program = parser.parse(query).get.asInstanceOf[Program]

}

object LetTest5FSeq extends DriverGene {

  override def loadTables(shred: Boolean = false, skew: Boolean = false): String =
    if (shred){
      s"""|val samples = spark.table("samples")
          |val IBag_samples__D = samples
          |IBag_samples__D.cache; IBag_samples__D.count
          |val copynumber = spark.table("copynumber")
          |val IBag_copynumber__D = copynumber
          |IBag_copynumber__D.cache; IBag_copynumber__D.count
          |val odict1 = spark.table("fodict1")
          |val IBag_occurrences__D = odict1
          |IBag_occurrences__D.cache; IBag_occurrences__D.count
          |// issue with partial shredding here
          |val odict2 = spark.table("fodict2").drop("flags")
          |val IMap_occurrences__D_transcript_consequences = odict2
          |IMap_occurrences__D_transcript_consequences.cache; IMap_occurrences__D_transcript_consequences.count
          |val odict3 = spark.table("fodict3")
          |val IMap_occurrences__D_transcript_consequences_consequence_terms = odict3
          |IMap_occurrences__D_transcript_consequences_consequence_terms.cache
          |IMap_occurrences__D_transcript_consequences_consequence_terms.count
          |""".stripMargin
    }else{
      s"""|val samples = spark.table("samples")
          |
          |val copynumber = spark.table("copynumber")
          |
          |val occurrences = spark.table("foccurrences")
          |""".stripMargin
    }

  val name = "LetTest5FSeq"
  
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
       InitScores <= $initScores;

       LetTest5 <= 
       for i in InitScores union
         for h in i.hybrid_scores union 
           {(hybrid_gene := h.hybrid_gene, hybrid_score := h.hybrid_score)}
     """

    val parser = Parser(tbls)
    val program = parser.parse(query).get.asInstanceOf[Program]

}

object LetTest5Seq extends DriverGene {

  override def loadTables(shred: Boolean = false, skew: Boolean = false): String =
    if (shred){
      s"""|val samples = spark.table("samples")
          |val IBag_samples__D = samples
          |IBag_samples__D.cache; IBag_samples__D.count
          |val copynumber = spark.table("copynumber")
          |val IBag_copynumber__D = copynumber
          |IBag_copynumber__D.cache; IBag_copynumber__D.count
          |val odict1 = spark.table("fodict1")
          |val IBag_occurrences__D = odict1
          |IBag_occurrences__D.cache; IBag_occurrences__D.count
          |// issue with partial shredding here
          |val odict2 = spark.table("fodict2").drop("flags")
          |val IMap_occurrences__D_transcript_consequences = odict2
          |IMap_occurrences__D_transcript_consequences.cache; IMap_occurrences__D_transcript_consequences.count
          |val odict3 = spark.table("fodict3")
          |val IMap_occurrences__D_transcript_consequences_consequence_terms = odict3
		  |IMap_occurrences__D_transcript_consequences_consequence_terms.cache
		  |IMap_occurrences__D_transcript_consequences_consequence_terms.count
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
                for c in cnvTmp union 
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

object LetTest5b extends DriverGene {

  override def loadTables(shred: Boolean = false, skew: Boolean = false): String =
    if (shred){
      s"""|val samples = spark.table("samples")
          |val IBag_samples__D = samples
          |IBag_samples__D.cache; IBag_samples__D.count
          |val copynumber = spark.table("copynumber")
          |val IBag_copynumber__D = copynumber
          |IBag_copynumber__D.cache; IBag_copynumber__D.count
          |val odict1 = spark.table("fodict1")
          |val IBag_occurrences__D = odict1
          |IBag_occurrences__D.cache; IBag_occurrences__D.count
          |// issue with partial shredding here
          |val odict2 = spark.table("fodict2").drop("flags")
          |val IMap_occurrences__D_transcript_consequences = odict2
          |IMap_occurrences__D_transcript_consequences.cache; IMap_occurrences__D_transcript_consequences.count
          |val odict3 = spark.table("fodict3")
          |val IMap_occurrences__D_transcript_consequences_consequence_terms = odict3
      |IMap_occurrences__D_transcript_consequences_consequence_terms.cache
      |IMap_occurrences__D_transcript_consequences_consequence_terms.count
          |""".stripMargin
    }else{
      s"""|val samples = spark.table("samples")
          |
          |val copynumber = spark.table("copynumber")
          |
          |val occurrences = spark.table("foccurrences")
          |""".stripMargin
    }

  val name = "LetTest5b"
  
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
              dedup(for t in o.transcript_consequences union 
                for c in $cnvs union 
                  if (o.donorId = c.sid && t.gene_id = c.gene)
                  then {(x := t.gene_id, hybrid_gene := t.gene_id, hybrid_score := (c.cnum + 0.01) * $imp )})
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

    val parser = Parser(tbls)
    val program = parser.parse(query).get.asInstanceOf[Program]

}

object LetTest5bSeq extends DriverGene {

  override def loadTables(shred: Boolean = false, skew: Boolean = false): String =
    if (shred){
      s"""|val samples = spark.table("samples")
          |val IBag_samples__D = samples
          |IBag_samples__D.cache; IBag_samples__D.count
          |val copynumber = spark.table("copynumber")
          |val IBag_copynumber__D = copynumber
          |IBag_copynumber__D.cache; IBag_copynumber__D.count
          |val odict1 = spark.table("fodict1")
          |val IBag_occurrences__D = odict1
          |IBag_occurrences__D.cache; IBag_occurrences__D.count
          |// issue with partial shredding here
          |val odict2 = spark.table("fodict2").drop("flags")
          |val IMap_occurrences__D_transcript_consequences = odict2
          |IMap_occurrences__D_transcript_consequences.cache; IMap_occurrences__D_transcript_consequences.count
          |val odict3 = spark.table("fodict3")
          |val IMap_occurrences__D_transcript_consequences_consequence_terms = odict3
          |IMap_occurrences__D_transcript_consequences_consequence_terms.cache
          |IMap_occurrences__D_transcript_consequences_consequence_terms.count
          |""".stripMargin
    }else{
      s"""|val samples = spark.table("samples")
          |
          |val copynumber = spark.table("copynumber")
          |
          |val occurrences = spark.table("occurrences")
          |""".stripMargin
    }

  val name = "LetTest5bSeq"
  
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
              dedup(for t in o.transcript_consequences union 
                for c in cnvTmp union 
                  if (o.donorId = c.sid && t.gene_id = c.gene)
                  then {(hybrid_gene := t.gene_id, hybrid_score := (c.cnum + 0.01) * $imp )})
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

object LetTest7a extends DriverGene {

  override def loadTables(shred: Boolean = false, skew: Boolean = false): String =
    if (shred){
      s"""|val samples = spark.table("samples")
          |val IBag_samples__D = samples
          |IBag_samples__D.cache; IBag_samples__D.count
          |
          |val copynumber = spark.table("copynumber")
          |val IBag_copynumber__D = copynumber
          |IBag_copynumber__D.cache; IBag_copynumber__D.count
          |
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
          dedup(for c in copynumber union 
            if (s.bcr_aliquot_uuid = c.cn_aliquot_uuid)
            then {( cn_gene_id := c.cn_gene_id, cn_copy_number := c.cn_copy_number + 0.001 )})
        )}
     """

    val query = 
      s"""
        LetTest7 <= 
        for t in $agg1 union 
          for x in t.cnvs union 
            {( bcr_patient_uuid := t.bcr_patient_uuid, cn_gene_id := x.cn_gene_id, cn_copy_number := x.cn_copy_number + 0.001 )}

      """

    val parser = Parser(tbls)
    val program = parser.parse(query).get.asInstanceOf[Program]
}

object LetTest7aSeq extends DriverGene {

  override def loadTables(shred: Boolean = false, skew: Boolean = false): String =
    if (shred){
      s"""|val samples = spark.table("samples")
          |val IBag_samples__D = samples
          |IBag_samples__D.cache; IBag_samples__D.count
          |
          |val copynumber = spark.table("copynumber")
          |val IBag_copynumber__D = copynumber
          |IBag_copynumber__D.cache; IBag_copynumber__D.count
          |
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
          dedup(for c in copynumber union 
            if (s.bcr_aliquot_uuid = c.cn_aliquot_uuid)
            then {( cn_gene_id := c.cn_gene_id, cn_copy_number := c.cn_copy_number + 0.001 )})
        )}
     """

    val query = 
      s"""
        Agg1 <= $agg1;

        LetTest7 <= 
        for t in Agg1 union 
          for x in t.cnvs union 
            {(bcr_patient_uuid := t.bcr_patient_uuid, cn_gene_id := x.cn_gene_id, cn_copy_number := x.cn_copy_number + 0.001 )}

      """

    val parser = Parser(tbls)
    val program = parser.parse(query).get.asInstanceOf[Program]
}

object LetTest7b extends DriverGene {

  override def loadTables(shred: Boolean = false, skew: Boolean = false): String =
    if (shred){
      s"""|val samples = spark.table("samples")
          |val IBag_samples__D = samples
          |IBag_samples__D.cache; IBag_samples__D.count
          |
          |val copynumber = spark.table("copynumber")
          |val IBag_copynumber__D = copynumber
          |IBag_copynumber__D.cache; IBag_copynumber__D.count
          |
          |""".stripMargin
    }else{
      s"""|val samples = spark.table("samples")
          |
          |val copynumber = spark.table("copynumber")
          |
          |val occurrences = spark.table("occurrences")
          |""".stripMargin
    }

  val name = "LetTest7b"
  
  val tbls = Map("occurrences" -> occurmids.tp, 
                  "copynumber" -> copynum.tp, 
                  "samples" -> samples.tp,
                  "network" -> network.tp, 
                  "genemap" -> gpmap.tp)

    val agg1 = 
     s"""
      for s in samples union 
        {( bcr_patient_uuid := s.bcr_patient_uuid, cnvs := 
          (for c in copynumber union 
            if (s.bcr_aliquot_uuid = c.cn_aliquot_uuid)
            then {( cn_gene_id := c.cn_gene_id, cnum := c.cn_copy_number + 0.001 )}).sumBy({cn_gene_id}, {cnum})
        )}
     """

    val query = 
      s"""
        LetTest7 <= 
        for t in $agg1 union 
          for x in t.cnvs union 
            {( bcr_patient_uuid := t.bcr_patient_uuid, cn_gene_id := x.cn_gene_id, cnum2 := x.cnum )}

      """

    val parser = Parser(tbls)
    val program = parser.parse(query).get.asInstanceOf[Program]
}

object LetTest7bSeq extends DriverGene {

  override def loadTables(shred: Boolean = false, skew: Boolean = false): String =
    if (shred){
      s"""|val samples = spark.table("samples")
          |val IBag_samples__D = samples
          |IBag_samples__D.cache; IBag_samples__D.count
          |
          |val copynumber = spark.table("copynumber")
          |val IBag_copynumber__D = copynumber
          |IBag_copynumber__D.cache; IBag_copynumber__D.count
          |
          |""".stripMargin
    }else{
      s"""|val samples = spark.table("samples")
          |
          |val copynumber = spark.table("copynumber")
          |
          |val occurrences = spark.table("occurrences")
          |""".stripMargin
    }
  val name = "LetTest7bSeq"
  
  val tbls = Map("occurrences" -> occurmids.tp, 
                  "copynumber" -> copynum.tp, 
                  "samples" -> samples.tp,
                  "network" -> network.tp, 
                  "genemap" -> gpmap.tp)


    val agg1 = 
     s"""
      for s in samples union 
        {( bcr_patient_uuid := s.bcr_patient_uuid, cnvs := 
          (for c in copynumber union 
            if (s.bcr_aliquot_uuid = c.cn_aliquot_uuid)
            then {( cn_gene_id := c.cn_gene_id, cnum := c.cn_copy_number + 0.001 )}).sumBy({cn_gene_id},{cnum})
        )}
     """

    val query = 
      s"""
        Agg1 <= $agg1;

        LetTest7 <= 
        for t in Agg1 union 
          for x in t.cnvs union 
            {(bcr_patient_uuid := t.bcr_patient_uuid, cn_gene_id := x.cn_gene_id, cnum2 := x.cnum )}

      """

    val parser = Parser(tbls)
    val program = parser.parse(query).get.asInstanceOf[Program]
}

object LetTest8 extends DriverGene {

  override def loadTables(shred: Boolean = false, skew: Boolean = false): String =
    if (shred){
      s"""|val samples = spark.table("samples")
          |val IBag_samples__D = samples
          |IBag_samples__D.cache; IBag_samples__D.count
          |
          |val copynumber = spark.table("copynumber")
          |val IBag_copynumber__D = copynumber
          |IBag_copynumber__D.cache; IBag_copynumber__D.count
          |
          |val IBag_pathways__D = spark.table("pathtop")
          |IBag_pathways__D.cache; IBag_pathways__D.count
          |
          |val IMap_pathways__D_gene_set = spark.table("pathdict")
          |IMap_pathways__D_gene_set.cache; IMap_pathways__D_gene_set.count
          |
          |val IBag_gtfmap__D = spark.table("gtfmap")
          |IBag_gtfmap__D.cache; IBag_gtfmap__D.count
          |""".stripMargin
    }else{
      s"""|val samples = spark.table("samples")
          |
          |val copynumber = spark.table("copynumber")
          |
          |val occurrences = spark.table("occurrences")
          |""".stripMargin
    }
  val name = "LetTest8"
  
  val tbls = Map("occurrences" -> occurmids.tp, 
                  "copynumber" -> copynum.tp, 
                  "samples" -> samples.tp,
                  "network" -> network.tp, 
                  "gtfmap" -> BagType(gtfType), 
                  "pathways" -> pathway.tp)

      val samps = 
      s"""
        dedup(for s in samples union 
          {(bcr_patient_uuid_d := s.bcr_patient_uuid)})
      """

    val fpath = 
      s"""
        for p in pathways union 
          for g in p.gene_set union 
            for g2 in gtfmap union 
              if (g.name = g2.g_gene_name)
              then {( pname := p.p_name, pgid := g2.g_gene_id )}
      """

    val cnvJoin = 
      s"""
        for s2 in samples union 
          for c in copynumber union
            if (s2.bcr_aliquot_uuid = c.cn_aliquot_uuid)
            then {(bcr_patient_uuid_1 := s2.bcr_patient_uuid, gid1 := c.cn_gene_id, cnum1 := c.cn_copy_number + 0.001 )}       
      """

    val agg1 = 
     s"""
      for a in $samps union 
        {( bcr_patient_uuid := a.bcr_patient_uuid_d, cnvs := 
          (for s1 in $cnvJoin union  
            if (s1.bcr_patient_uuid_1 = a.bcr_patient_uuid_d)
            then for g in PWays union 
              if (s1.gid1 = g.pgid)
              then {( path := g.pname, cnum := s1.cnum1 + 0.001 )}).sumBy({path},{cnum})
        )}
     """

    val query = 
      s"""
        PWays <= $fpath; 

        LetTest7 <= 
        for t in $agg1 union 
          for x in t.cnvs union 
            {(bcr_patient_uuid := t.bcr_patient_uuid, path := x.path, cnum2 := x.cnum )}

      """

    val parser = Parser(tbls)
    val program = parser.parse(query).get.asInstanceOf[Program]
}

object LetTest8Seq extends DriverGene {

  override def loadTables(shred: Boolean = false, skew: Boolean = false): String =
    if (shred){
      s"""|val samples = spark.table("samples")
          |val IBag_samples__D = samples
          |IBag_samples__D.cache; IBag_samples__D.count
          |
          |val copynumber = spark.table("copynumber")
          |val IBag_copynumber__D = copynumber
          |IBag_copynumber__D.cache; IBag_copynumber__D.count
          |
          |val IBag_pathways__D = spark.table("pathtop")
          |IBag_pathways__D.cache; IBag_pathways__D.count
          |
          |val IMap_pathways__D_gene_set = spark.table("pathdict")
          |IMap_pathways__D_gene_set.cache; IMap_pathways__D_gene_set.count
          |
          |val IBag_gtfmap__D = spark.table("gtfmap")
          |IBag_gtfmap__D.cache; IBag_gtfmap__D.count
          |""".stripMargin
    }else{
      s"""|val samples = spark.table("samples")
          |
          |val copynumber = spark.table("copynumber")
          |
          |val occurrences = spark.table("occurrences")
          |""".stripMargin
    }
  val name = "LetTest8Seq"
  
  val tbls = Map("occurrences" -> occurmids.tp, 
                  "copynumber" -> copynum.tp, 
                  "samples" -> samples.tp,
                  "network" -> network.tp, 
                  "gtfmap" -> BagType(gtfType), 
                  "pathways" -> pathway.tp)

    val samps = 
      s"""
        dedup(for s in samples union 
          {(bcr_patient_uuid_d := s.bcr_patient_uuid)})
      """

    val fpath = 
      s"""
        for p in pathways union 
          for g in p.gene_set union 
            for g2 in gtfmap union 
              if (g.name = g2.g_gene_name)
              then {( pname := p.p_name, pgid := g2.g_gene_id )}
      """

    val cnvJoin = 
      s"""
        for s in samples union 
          for c in copynumber union
            if (s.bcr_aliquot_uuid = c.cn_aliquot_uuid)
            then {(bcr_patient_uuid_1 := s.bcr_patient_uuid, gid1 := c.cn_gene_id, cnum1 := c.cn_copy_number + 0.001 )}       
      """

    val agg1 = 
     s"""
      for a in DSamples union 
        {( bcr_patient_uuid := a.bcr_patient_uuid_d, cnvs := 
          (for s1 in cnvJoin union  
            if (s1.bcr_patient_uuid_1 = a.bcr_patient_uuid_d)
            then for g in PWays union 
              if (s1.gid1 = g.pgid)
              then {( path := g.pname, cnum := s1.cnum1 + 0.001 )}).sumBy({path},{cnum})
        )}
     """

    val query = 
      s"""
        DSamples <= $samps;

        PWays <= $fpath; 

        cnvJoin <= $cnvJoin; 

        Agg1 <= $agg1;

        LetTest7 <= 
        for t in Agg1 union 
          for x in t.cnvs union 
            {(bcr_patient_uuid := t.bcr_patient_uuid, path := x.path, cnum2 := x.cnum )}

      """

    val parser = Parser(tbls)
    val program = parser.parse(query).get.asInstanceOf[Program]
}

object LetTest9 extends DriverGene {

  override def loadTables(shred: Boolean = false, skew: Boolean = false): String =
    if (shred){
      s"""|val samples = spark.table("samples")
          |val IBag_samples__D = samples
          |IBag_samples__D.cache; IBag_samples__D.count
          |
          |val copynumber = spark.table("copynumber")
          |val IBag_copynumber__D = copynumber
          |IBag_copynumber__D.cache; IBag_copynumber__D.count
          |
          |val IBag_pathways__D = spark.table("pathtop")
          |IBag_pathways__D.cache; IBag_pathways__D.count
          |
          |val IMap_pathways__D_gene_set = spark.table("pathdict")
          |IMap_pathways__D_gene_set.cache; IMap_pathways__D_gene_set.count
          |
          |val IBag_gtfmap__D = spark.table("gtfmap")
          |IBag_gtfmap__D.cache; IBag_gtfmap__D.count
          |""".stripMargin
    }else{
      s"""|val samples = spark.table("samples")
          |
          |val copynumber = spark.table("copynumber")
          |
          |val occurrences = spark.table("occurrences")
          |""".stripMargin
    }
  val name = "LetTest9"
  
  val tbls = Map("occurrences" -> occurmids.tp, 
                  "copynumber" -> copynum.tp, 
                  "samples" -> samples.tp,
                  "network" -> network.tp, 
                  "gtfmap" -> BagType(gtfType), 
                  "pathways" -> pathway.tp)

      val samps = 
      s"""
        dedup(for s in samples union 
          {(bcr_patient_uuid_d := s.bcr_patient_uuid)})
      """

    val fpath = 
      s"""
        for p in pathways union 
          for g in p.gene_set union 
            for g2 in gtfmap union 
              if (g.name = g2.g_gene_name)
              then {( pname := p.p_name, pgid := g2.g_gene_id )}
      """

    val cnvJoin = 
      s"""
        for s2 in samples union 
          for c in copynumber union
            if (s2.bcr_aliquot_uuid = c.cn_aliquot_uuid)
            then {(bcr_patient_uuid_1 := s2.bcr_patient_uuid, gid1 := c.cn_gene_id, cnum1 := c.cn_copy_number + 0.001 )}       
      """

    val agg1 = 
     s"""
      for s in samples union 
        {( bcr_patient_uuid := s.bcr_patient_uuid, cnvs := 
          (for c in copynumber union  
            if (s.bcr_aliquot_uuid = c.cn_aliquot_uuid)
            then for g in PWays union 
              if (c.cn_gene_id = g.pgid)
              then {( path := g.pname, cnum := c.cn_copy_number + 0.001 )}).sumBy({path},{cnum})
        )}
     """

    val query = 
      s"""
        PWays <= $fpath; 

        LetTest7 <= 
        for t in $agg1 union 
          for x in t.cnvs union 
            {(bcr_patient_uuid := t.bcr_patient_uuid, path := x.path, cnum2 := x.cnum )}

      """

    val parser = Parser(tbls)
    val program = parser.parse(query).get.asInstanceOf[Program]
}

object LetTest9Seq extends DriverGene {

  override def loadTables(shred: Boolean = false, skew: Boolean = false): String =
    if (shred){
      s"""|val samples = spark.table("samples")
          |val IBag_samples__D = samples
          |IBag_samples__D.cache; IBag_samples__D.count
          |
          |val copynumber = spark.table("copynumber")
          |val IBag_copynumber__D = copynumber
          |IBag_copynumber__D.cache; IBag_copynumber__D.count
          |
          |val IBag_pathways__D = spark.table("pathtop")
          |IBag_pathways__D.cache; IBag_pathways__D.count
          |
          |val IMap_pathways__D_gene_set = spark.table("pathdict")
          |IMap_pathways__D_gene_set.cache; IMap_pathways__D_gene_set.count
          |
          |val IBag_gtfmap__D = spark.table("gtfmap")
          |IBag_gtfmap__D.cache; IBag_gtfmap__D.count
          |""".stripMargin
    }else{
      s"""|val samples = spark.table("samples")
          |
          |val copynumber = spark.table("copynumber")
          |
          |val occurrences = spark.table("occurrences")
          |""".stripMargin
    }
  val name = "LetTest9Seq"
  
  val tbls = Map("occurrences" -> occurmids.tp, 
                  "copynumber" -> copynum.tp, 
                  "samples" -> samples.tp,
                  "network" -> network.tp, 
                  "gtfmap" -> BagType(gtfType), 
                  "pathways" -> pathway.tp)

    val samps = 
      s"""
        dedup(for s in samples union 
          {(bcr_patient_uuid_d := s.bcr_patient_uuid)})
      """

    val fpath = 
      s"""
        for p in pathways union 
          for g in p.gene_set union 
            for g2 in gtfmap union 
              if (g.name = g2.g_gene_name)
              then {( pname := p.p_name, pgid := g2.g_gene_id )}
      """

    val agg1 = 
     s"""
      for s in samples union 
        {( bcr_patient_uuid := s.bcr_patient_uuid, cnvs := 
          (for c in copynumber union  
            if (s.bcr_aliquot_uuid = c.cn_aliquot_uuid)
            then for g in PWays union 
              if (c.cn_gene_id = g.pgid)
              then {( path := g.pname, cnum := c.cn_copy_number + 0.001 )}).sumBy({path},{cnum})
        )}
     """

    val query = 
      s"""

        PWays <= $fpath; 

        Agg1 <= $agg1;

        LetTest7 <= 
        for t in Agg1 union 
          for x in t.cnvs union 
            {(bcr_patient_uuid := t.bcr_patient_uuid, path := x.path, cnum2 := x.cnum )}

      """

    val parser = Parser(tbls)
    val program = parser.parse(query).get.asInstanceOf[Program]
}

object LetTest0 {
  def apply(letOpt: Boolean = false): Query = new LetTest0(letOpt)
  def apply(): LetTest0 = new LetTest0()
}
