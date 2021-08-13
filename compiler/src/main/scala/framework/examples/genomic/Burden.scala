package framework.examples.genomic

import framework.common._
import framework.examples.Query
import framework.nrc.Parser

object GeneBurden0 extends GenomicSchema {

  val name = "GeneBurden0"
  

  val tbls = Map("genes" -> BagType(gtfType), 
                 "vcf" -> BagType(variantType))

  val burden = 
    s"""
      Burden <=
        (for v in vcf union 
          for g in genes union 
            if (v.contig = g.g_contig && v.start >= g.g_start && g.g_end >= v.start)
            then for c in v.genotypes union 
              {(sample := c.g_sample, gene := g.g_gene_name, burden := c.call)}).sumBy({sample, gene}, {burden})
	"""
    // Groups <= (Burden).groupBy({sample}, {gene, burden}, "burdens")
    //"""

    val parser = Parser(tbls)
    val program: Program = parser.parse(burden).get.asInstanceOf[Program]

}

object GeneBurden extends GenomicSchema {

  val name = "GeneBurden"
  

  val tbls = Map("gtf" -> BagType(gtfType), 
                 "variants" -> BagType(variantType))

  val burden = 
    s""" 
      Burden <=
        for g in gtf union 
          {(gene := g.g_gene_name, burdens := 
            (for v in variants union 
              if (v.contig = g.g_contig && v.start >= g.g_start && g.g_end >= v.start)
                then for c in v.genotypes union 
                  {(sample := c.g_sample, burden := c.call)}).sumBy({sample}, {burden})
            )}
    """

    val parser = Parser(tbls)
    val program: Program = parser.parse(burden).get.asInstanceOf[Program]

}

object GeneBurdenS extends GenomicSchema {

  val name = "GeneBurdenS"
  

  val tbls = Map("genes" -> BagType(gtfType), 
                 "vcf" -> BagType(variantType))

  val burden = 
    s""" 
      GeneTags <= for g in genes union
        for v in vcf union 
          if (v.contig = g.g_contig && v.start >= g.g_start && g.g_end >= v.start)
          then {(gene := g.g_gene_name, genotypes := 
            for c in v.genotypes union 
              {(sample := c.g_sample, call := c.call)} )};

      Burden <= 
        (for g in GeneTags union 
          for c in g.genotypes union 
            {(gene := g.gene, sample := c.sample, burden := c.call)}).sumBy({gene, sample}, {burden});

      Groups <= (Burden).groupBy({sample}, {gene, burden}, "burdens")

    """

    val parser = Parser(tbls)
    val program: Program = parser.parse(burden).get.asInstanceOf[Program]

}

