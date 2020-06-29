package framework.examples

import framework.common._

trait GenomicSchema extends Query{

    // this overrides a function that was previously used for tpch benchmarking
    // just define how to load your inputs here
    // I think there was a function in Query previously then...
    def loadTables(shred: Boolean = false, skew: Boolean = false): String = {
    if (shred)
            s"""|val vloader = new VariantLoader(spark, "/home/yash/Documents/Data/Variants/sub_chr22.vcf")
                |val (variants, genotypes) = vloader.shredDS
                |val IBag_variants__D = variants
                |val IDict_variants__D_genotypes = genotypes
                |
                |val cloader = new TGenomesLoader(spark)
                |val IBag_metadata__D = cloader.load("/home/yash/Documents/Data/Phenotype/1000g.csv")
                |val gtfLoader = new GTFLoader(spark, "/home/yash/Documents/Data/Map/Homo_sapiens.GRCh37.87.chr.gtf")
                |val Gtfs = gtfLoader.loadDS
                |val IBag_Gtfs__D = Gtfs
                |
                |val pathwayLoader = new PathwayLoader(spark, "/home/yash/Documents/Data/Pathway/c2.cp.v7.1.symbols.gmt")
                |val (pathways, geneSet) = pathwayLoader.shredDS
                |val IBag_pathways__D = pathways
                |val IDict_pathways__D_gene_set = geneSet
                |""".stripMargin
        else
            s"""|val vloader = new VariantLoader(spark, "/home/yash/Documents/Data/Variants/sub_chr22.vcf")
                |val variants = vloader.loadDS
                |val cloader = new TGenomesLoader(spark)
                |val metadata = cloader.load("/home/yash/Documents/Data/Phenotype/1000g.csv")
                |val gtfLoader = new GTFLoader(spark, "/home/yash/Documents/Data/Map/Homo_sapiens.GRCh37.87.chr.gtf")
                |val Gtfs = gtfLoader.loadDS
                |val pathwayLoader = new PathwayLoader(spark, "/home/yash/Documents/Data/Pathway/c2.cp.v7.1.symbols.gmt")
                |val pathways = pathwayLoader.loadDS
                |""".stripMargin
    }


    // define the types, which would reflect the case classes from your variant loader
    val genoType = TupleType("g_sample" -> StringType, "call" -> DoubleType)
    val variantType = TupleType(
        "contig" -> StringType,
        "start" -> IntType,
        "reference" -> StringType,
        "alternate" -> StringType,
        "genotypes" -> BagType(genoType))
    val gtfType = TupleType(
        "g_contig" -> StringType,
        "g_start" -> IntType,
        "g_end" -> IntType,
        "gene_name" -> StringType
    )
    val geneType = TupleType("name" -> StringType)
    val pathwayType = TupleType("name" -> StringType, "url" -> StringType, "gene_set" -> BagType(geneType))

    val metaType = TupleType("m_sample" -> StringType, "family_id" -> StringType, "population" -> StringType, "gender" -> StringType)

    // define references to these types
    val variants = BagVarRef("variants", BagType(variantType))
    val vr = TupleVarRef("v", variantType)
    val gr = TupleVarRef("g", genoType)

    val gtfs = BagVarRef("Gtfs", BagType(gtfType))
    val gtfr = TupleVarRef("gtf", gtfType)

    val pathways = BagVarRef("pathways", BagType(pathwayType))
    val pr = TupleVarRef("p", pathwayType)
    val ger = TupleVarRef("ge", geneType)

    val metadata = BagVarRef("metadata", BagType(metaType))
    val mr = TupleVarRef("m", metaType)
}
    