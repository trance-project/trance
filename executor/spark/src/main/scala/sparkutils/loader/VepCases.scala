package sparkutils.loader

/** Various case classes for handling JSON data from VEP **/

case class Element(element: String)
case class Motif0(bp_overlap: Long, percentage_overlap: Double, impact: String, motif_feature_id: String, consequence_terms: Seq[String], variant_allele: String)
case class Motif(bp_overlap: Long, percentage_overlap: Double, impact: String, motif_feature_id: String, consequence_terms: Seq[Element], variant_allele: String)
case class Domains(db: String, name: String)
case class Transcript0(bp_overlap: Long, percentage_overlap: Double, gene_symbol_source: String, biotype: String, protein_end: Long, impact: String, gene_id: String, mane: String, 
  domains: Seq[Domains], cdna_start: Long, transcript_id: String, cdna_end: Long, exon: String, cds_end: Long, cds_start: Long, uniparc: Seq[String], 
  ccds: String, canonical: Long, intron: String, protein_id: String, consequence_terms: Seq[String], tsl: Long, strand: Long, swissprot: Seq[String], 
  hgnc_id: String, protein_start: Long, appris: String, variant_allele: String, distance: Long, gene_symbol: String)

case class Regulatory0(bp_overlap: Long, percentage_overlap: Double, regulatory_feature_id: String, impact: String, consequence_terms: Seq[String], variant_allele: String)
case class Regulatory(bp_overlap: Long, percentage_overlap: Double, regulatory_feature_id: String, impact: String, consequence_terms: Seq[Element], variant_allele: String)
case class Intergenic0(consequence_terms: Seq[String], impact: String, variant_allele: String)
case class Intergenic(consequence_terms: Seq[Element], impact: String, variant_allele: String)
case class AnnotatedVariant0(vreference: String, motif_feature_consequences: Seq[Motif0], allele_string: String, genotypes: Seq[Call], seq_region_name: String, 
  transcript_consequences: Seq[Transcript0], vid: String, regulatory_feature_consequences: Seq[Regulatory0], vcontig: String, assembly_name: String, 
  intergenic_consequences: Seq[Intergenic0], end: Long, variant_class: String, vstart: Int, input: String, most_severe_consequence: String)
case class AnnotatedVariant(vid: String, vreference: String, motif_feature_consequences: Seq[Motif0], allele_string: String, genotypes: Seq[Call], seq_region_name: String, 
  transcript_consequences: Seq[Transcript], regulatory_feature_consequences: Seq[Regulatory0], vcontig: String, assembly_name: String, 
  intergenic_consequences: Seq[Intergenic0], end: Long, variant_class: String, vstart: Int, input: String, most_severe_consequence: String)

case class VepAnnotation(motif_feature_consequences: Seq[Motif0], allele_string: String, seq_region_name: String, 
  transcript_consequences: Seq[Transcript0], regulatory_feature_consequences: Seq[Regulatory0], assembly_name: String, 
  intergenic_consequences: Seq[Intergenic0], end: Long, variant_class: String, input: String, most_severe_consequence: String)

case class Transcript(amino_acids: String, distance: Option[Long], cdna_end: Option[Long], cdna_start: Option[Long], cds_end: Option[Long], cds_start: Option[Long], codons: String, consequence_terms: List[String], flags: List[String], gene_id: String, impact: String, protein_end: Option[Long], protein_start: Option[Long], strand: Option[Long], transcript_id: String, variant_allele: String)

case class TranscriptQuant(amino_acids: String, distance: Option[Long], cdna_end: Option[Long], cdna_start: Option[Long], cds_end: Option[Long], cds_start: Option[Long], codons: String, consequence_terms: List[Double], flags: List[String], gene_id: String, impact: Double, protein_end: Option[Long], protein_start: Option[Long], strand: Option[Long], transcript_id: String, variant_allele: String)

case class Transcript3(aliquot_id: String, amino_acids: String, distance: Long, cdna_end: Long, cdna_start: Long, cds_end: Long, cds_start: Long, codons: String, consequence_terms: List[String], flags: List[String], gene_id: String, impact: String, protein_end: Long, protein_start: Long, ts_strand: Long, transcript_id: String, variant_allele: String)

case class Transcript2(impact: String, consequence_terms: Seq[String])

case class VepAnnotTrunc(vid: String, allele_string: String, assembly_name: String, end: Long, input: String, most_severe_consequence: String, seq_region_name: String, start: Long, strand: Long, transcript_consequences: Seq[Transcript])

case class VepAnnotTrunc2(input: String, transcript_consequences: Seq[Transcript2])

case class VariantVep(vid: String, vcontig: String, vstart: Int, vreference: String, genotypes: Seq[Call], vepCall: String)
case class VepOccurrence(donorId: String, oid: String, vend: Int, projectId: String, vstart: Int, Reference_Allele: String, Tumor_Seq_Allele1: String, Tumor_Seq_Allele2: String, chromosome: String, vepCall: String)

case class VepOccurrence2(donorId: String, oid: String, vend: Int, projectId: String, vstart: Int, Reference_Allele: String, Tumor_Seq_Allele1: String, Tumor_Seq_Allele2: String, chromosome: String)
case class VepOccurrence3(chromosome: String, vstart: Int, oid: String)


case class OccurrenceNulls(oid: String, donorId: String, vend: Int, projectId: String, vstart: Int, Reference_Allele: String, Tumor_Seq_Allele1: String, Tumor_Seq_Allele2: String, chromosome: String, allele_string: Option[String], assembly_name: Option[String], end: Option[Long], vid: Option[String], input: Option[String], most_severe_consequence: Option[String], seq_region_name: Option[String], start: Option[Long], strand: Option[Long], transcript_consequences: Option[Seq[Transcript]])

case class Occurrence(oid: String, donorId: String, vend: Long, projectId: String, vstart: Long, Reference_Allele: String, Tumor_Seq_Allele1: String, Tumor_Seq_Allele2: String, chromosome: String, allele_string: String, assembly_name: String, end: Long, vid: String, input: String, most_severe_consequence: String, seq_region_name: String, start: Long, strand: Long, transcript_consequences: Option[Seq[Transcript]])

case class OccurrenceBiospec(oid: String, donorId: String, vend: Long, projectId: String, vstart: Long, Reference_Allele: String, Tumor_Seq_Allele1: String, Tumor_Seq_Allele2: String, chromosome: String, allele_string: String, assembly_name: String, end: Long, vid: String, input: String, most_severe_consequence: String, seq_region_name: String, start: Long, strand: Long, transcript_consequences: Option[Seq[Transcript]],
  bcr_patient_uuid: String, bcr_sample_barcode: String, bcr_aliquot_barcode: String, bcr_aliquot_uuid: String, biospecimen_barcode_bottom: String, center_id: String, concentration: Double, date_of_shipment: String, is_derived_from_ffpe: String, plate_column: Int, plate_id: String, plate_row: String, quantity: Double, source_center: Int, volume: Double)

case class OccurrenceMapped(aliquot_id: String, oid: String, donorId: String, vend: Long, projectId: String, vstart: Long, Reference_Allele: String, Tumor_Seq_Allele1: String, Tumor_Seq_Allele2: String, chromosome: String, allele_string: String, assembly_name: String, end: Long, vid: String, input: String, most_severe_consequence: String, seq_region_name: String, start: Long, strand: Long, transcript_consequences: Option[Seq[Transcript]])

case class OccurrQuant(oid: String, donorId: String, vend: Long, projectId: String, vstart: Long, Reference_Allele: String, Tumor_Seq_Allele1: String, Tumor_Seq_Allele2: String, chromosome: String, allele_string: String, assembly_name: String, end: Long, vid: String, input: String, most_severe_consequence: String, seq_region_name: String, start: Long, strand: Long, transcript_consequences: Option[Seq[TranscriptQuant]])

case class Occurrence2(oid: String, aliquotId: String, donorId: String, vend: Long, projectId: String, vstart: Long, Reference_Allele: String, Tumor_Seq_Allele1: String, Tumor_Seq_Allele2: String, chromosome: String, allele_string: String, assembly_name: String, end: Long, vid: String, input: String, most_severe_consequence: String, seq_region_name: String, start: Long, strand: Long, transcript_consequences: Seq[Transcript3])

case class OccurrDict1(oid: String, aliquotId: String, donorId: String, vend: Long, projectId: String, vstart: Long, Reference_Allele: String, Tumor_Seq_Allele1: String, Tumor_Seq_Allele2: String, chromosome: String, allele_string: String, assembly_name: String, end: Long, vid: String, input: String, most_severe_consequence: String, seq_region_name: String, start: Long, strand: Long, transcript_consequences: String)

case class OccurrTransDict2(_1: String, aliquot_id: String, amino_acids: String, distance: Long, cdna_end: Long, cdna_start: Long, cds_end: Long, cds_start: Long, codons: String, consequence_terms: String, flags: List[String], gene_id: String, impact: String, protein_end: Long, protein_start: Long, ts_strand: Long, transcript_id: String, variant_allele: String)

case class OccurrTransConseqDict3(_1: String, element: String)

case class FreqStruct0(A: FreqStruct, T: FreqStruct)

case class FreqStruct(afr: Option[Double], amr: Option[Long], eas: Option[Long],
	eur: Option[Long], gnomad: Option[Double], gnomad_afr: Option[Double], 
		gnomad_amr: Option[Double], gnomad_asj: Option[Long], gnomad_eas: Option[Long],
			gnomad_fin: Option[Long], gnomad_nfe: Option[Double], gnomad_oth: Option[Long],
				gnomad_sas: Option[Long], sas: Option[Long])

case class Element0(allele_String: Option[String],
clin_sig: Option[Seq[Element]],
clin_sig_allele: Option[String],
end: Option[Long],
frequencies: Option[FreqStruct],
id: Option[String],
minor_allele: Option[String],
minor_allele_freq: Option[Double],
phenotype_or_disease: Option[Long],
pubmed: Option[Seq[String]], seq_region_name: Option[String], 
somatic: Option[Long], start: Option[Long], strand: Option[Long])

case class RFElement(biotype: Option[String], consequence_terms: Option[Seq[String]],
impact: Option[String],regulatory_feature_id: Option[String], variant_allele: Option[String])

case class TranscriptElement(amino_acids: Option[String],
appris: Option[String], biotype: Option[String],
canonical: Option[Long], ccds: Option[String],
cdna_end: Option[Long], cdna_start: Option[Long],
cds_end: Option[Long], cds_start: Option[Long],
codons: Option[String],
consequence_terms: Option[Seq[String]],
distance: Option[Long],
domains: Option[Seq[Domains]],
exon: Option[String],
flags: Option[Seq[String]],
gene_id: Option[String],
gene_pheno: Option[Long],
gene_symbol: Option[String],
gene_symbol_source: Option[String],
hgnc_id: Option[String],
impact: Option[String],
intron: Option[String],
mane: Option[String],
polyphen_prediction: Option[String],
polyphen_score: Option[Double],
protein_end: Option[Long],
protein_id: Option[String],
protein_start: Option[Long],
ift_prediction: Option[String],
sift_score: Option[Double],
strand: Option[Long],
swissprot: Option[Seq[String]],
transcript_id: Option[String],
trembl: Option[Seq[String]],
tsl: Option[Long],
uniparc: Option[Seq[String]],
variant_allele: Option[String])

case class OccurrenceEverything(allele_String: Option[String],
assembly_name: Option[String], colocated_variants: Option[Seq[Element0]],
end: Option[Long], id: Option[String], input: Option[String],
most_severe_consequence: Option[String],
regulatory_feature_consequences: Option[Seq[RFElement]], 
seq_region_name: Option[String], start: Option[Long], 
strand: Option[Long], 
transcript_consequences: Option[Seq[TranscriptElement]],
variant_class: Option[String], vid: String)

case class CoLocated(AA: Option[String],
AFR: Option[String],
AMR: Option[String],
EA: Option[String],
EAS: Option[String],
EUR: Option[String],
SAS: Option[String],
allele_string: Option[String],
clin_sig: Option[Seq[String]],
clin_sig_allele: Option[String],
end: Option[Long],
gnomAD: Option[String],
gnomAD_AFR: Option[String],
gnomAD_AMR: Option[String],
gnomAD_ASJ: Option[String],
gnomAD_EAS: Option[String],
gnomAD_FIN: Option[String],
gnomAD_NFE: Option[String],
gnomAD_OTH: Option[String],
gnomAD_SAS: Option[String],
id: Option[String],
minor_allele: Option[String],
minor_allele_freq: Option[Double],
phenotype_or_disease: Option[Long],
pubmed: Option[Seq[Long]],
seq_region_name: Option[String],
somatic: Option[Long],
start: Option[Long],
strand: Option[Long])

case class CoLocated2(
case_id: String,
AA: String,
AFR: String,
AMR: String,
EA: String,
EAS: String,
EUR: String,
SAS: String,
allele_string: String,
clin_sig: Seq[String],
clin_sig_allele: String,
cv_end: Long,
gnomAD: String,
gnomAD_AFR: String,
gnomAD_AMR: String,
gnomAD_ASJ: String,
gnomAD_EAS: String,
gnomAD_FIN: String,
gnomAD_NFE: String,
gnomAD_OTH: String,
gnomAD_SAS: String,
cid: String,
minor_allele: String,
minor_allele_freq: Double,
phenotype_or_disease: Long,
pubmed: Seq[Long],
seq_region_name: String,
somatic: Long,
cv_start: Long,
cv_strand: Long)


case class TranscriptFull(amino_acids: Option[String],
cdna_end: Option[Long],
cdna_start: Option[Long],
cds_end: Option[Long],
cds_start: Option[Long],
codons: Option[String],
consequence_terms: Option[Seq[String]],
distance: Option[Long],
exon: Option[String],
flags: Option[Seq[String]],
gene_id: Option[String],
impact: Option[String],
intron: Option[String],
polyphen_prediction: Option[String],
polyphen_score: Option[Double],
protein_end: Option[Long],
protein_start: Option[Long],
sift_prediction: Option[String],
sift_score: Option[Double],
strand: Option[Long],
transcript_id: Option[String],
variant_allele: Option[String])

case class TranscriptFull2(
  case_id: String,
amino_acids: String,
cdna_end: Long,
cdna_start: Long,
cds_end: Long,
cds_start: Long,
codons: String,
consequence_terms: Seq[String],
distance: Long,
exon: String,
flags: Seq[String],
gene_id: String,
impact: String,
intron: String,
polyphen_prediction: String,
polyphen_score: Double,
protein_end: Long,
protein_start: Long,
sift_prediction: String,
sift_score: Double,
ts_strand: Long,
transcript_id: String,
variant_allele: String)

case class VepTranscriptFull(
amino_acids: String,
cdna_end: Long,
cdna_start: Long,
cds_end: Long,
cds_start: Long,
codons: String,
consequence_terms: Seq[String],
distance: Long,
exon: String,
flags: Seq[String],
gene_id: String,
impact: String,
intron: String,
polyphen_prediction: String,
polyphen_score: Double,
protein_end: Long,
protein_start: Long,
sift_prediction: String,
sift_score: Double,
ts_strand: Long,
transcript_id: String,
variant_allele: String)

case class VepDict1(vid: String, allele_string: String, assembly_name: String, 
end: Long, id: String, input: String, most_severe_consequence: String, seq_region_name: String, 
start: Long, strand: Long, transcript_consequences: String)

case class VepDict2(
_1: String,
amino_acids: String,
cdna_end: Long,
cdna_start: Long,
cds_end: Long,
cds_start: Long,
codons: String,
consequence_terms: String,
distance: Long,
exon: String,
flags: Seq[String],
gene_id: String,
impact: String,
intron: String,
polyphen_prediction: String,
polyphen_score: Double,
protein_end: Long,
protein_start: Long,
sift_prediction: String,
sift_score: Double,
ts_strand: Long,
transcript_id: String,
variant_allele: String)

case class VepDict3(_1: String, element: String)

case class OccurTransDict2(
_1: String,
//case_id: String,
amino_acids: String,
cdna_end: Long,
cdna_start: Long,
cds_end: Long,
cds_start: Long,
codons: String,
consequence_terms: String,
distance: Long,
//exon: String,
//flags: Seq[String],
gene_id: String,
impact: String,
//intron: String,
//polyphen_prediction: String,
//polyphen_score: Double,
protein_end: Long,
protein_start: Long,
//sift_prediction: String,
//sift_score: Double,
ts_strand: Long,
transcript_id: String,
variant_allele: String)


case class OccurTransDict2Mid(
_1: String,
case_id: String,
amino_acids: String,
cdna_end: Long,
cdna_start: Long,
cds_end: Long,
cds_start: Long,
codons: String,
consequence_terms: String,
distance: Long,
exon: String,
// flags: Seq[String],
gene_id: String,
impact: String,
intron: String,
polyphen_prediction: String,
polyphen_score: Double,
protein_end: Long,
protein_start: Long,
sift_prediction: String,
sift_score: Double,
ts_strand: Long,
transcript_id: String,
variant_allele: String)

case class VepAnnotFull(vid: String, allele_string: String, assembly_name: String, 
colocated_variants: Option[Seq[CoLocated]],
end: Long, id: String, input: String, most_severe_consequence: String, seq_region_name: String, 
start: Long, strand: Long, transcript_consequences: Option[Seq[TranscriptFull]])

case class VepAnnotMid(vid: String, allele_string: String, assembly_name: String, 
end: Long, id: String, input: String, most_severe_consequence: String, seq_region_name: String, 
start: Long, strand: Long, transcript_consequences: Option[Seq[TranscriptFull]])

case class VepAnnotationFull(vid: String, allele_string: String, assembly_name: String, 
end: Long, id: String, input: String, most_severe_consequence: String, seq_region_name: String, 
start: Long, strand: Long, transcript_consequences: Seq[VepTranscriptFull])

case class VepAnnotFull2(vid: String, allele_string: String, assembly_name: String, 
colocated_variants: Seq[CoLocated2],
end: Long, id: String, input: String, most_severe_consequence: String, seq_region_name: String, 
start: Long, strand: Long, transcript_consequences: Seq[TranscriptFull2])

case class VepAnnotMid2(vid: String, allele_string: String, assembly_name: String, 
end: Long, id: String, input: String, most_severe_consequence: String, seq_region_name: String, 
start: Long, strand: Long, transcript_consequences: Seq[TranscriptFull2])

case class OccurrenceFullNulls(oid: String, donorId: String, vend: Int, projectId: String, vstart: Int, Reference_Allele: String, Tumor_Seq_Allele1: String, Tumor_Seq_Allele2: String, chromosome: String, allele_string: Option[String], assembly_name: Option[String], end: Option[Long], vid: Option[String], input: Option[String], most_severe_consequence: Option[String], seq_region_name: Option[String], start: Option[Long], strand: Option[Long], colocated_variants: Option[Seq[CoLocated]], transcript_consequences: Option[Seq[TranscriptFull]])

case class OccurrenceFullPartialNulls(oid: String, donorId: String, vend: Long, projectId: String, vstart: Long, Reference_Allele: String, Tumor_Seq_Allele1: String, Tumor_Seq_Allele2: String, chromosome: String, allele_string: String, assembly_name: String, end: Long, vid: String, input: String, most_severe_consequence: String, seq_region_name: String, start: Long, strand: Long, colocated_variants: Option[Seq[CoLocated]], transcript_consequences: Option[Seq[TranscriptFull]])

case class OccurrenceFull(oid: String, donorId: String, vend: Long, projectId: String, vstart: Long, Reference_Allele: String, Tumor_Seq_Allele1: String, Tumor_Seq_Allele2: String, chromosome: String, allele_string: String, assembly_name: String, end: Long, vid: String, input: String, most_severe_consequence: String, seq_region_name: String, start: Long, strand: Long, colocated_variants: Seq[CoLocated2], transcript_consequences: Seq[TranscriptFull2])

case class OccurrenceMidNulls(oid: String, donorId: String, vend: Int, projectId: String, vstart: Int, Reference_Allele: String, Tumor_Seq_Allele1: String, Tumor_Seq_Allele2: String, chromosome: String, allele_string: Option[String], assembly_name: Option[String], end: Option[Long], vid: Option[String], input: Option[String], most_severe_consequence: Option[String], seq_region_name: Option[String], start: Option[Long], strand: Option[Long], transcript_consequences: Option[Seq[TranscriptFull]])

case class OccurrenceMidPartialNulls(oid: String, donorId: String, vend: Long, projectId: String, vstart: Long, Reference_Allele: String, Tumor_Seq_Allele1: String, Tumor_Seq_Allele2: String, chromosome: String, allele_string: String, assembly_name: String, end: Long, vid: String, input: String, most_severe_consequence: String, seq_region_name: String, start: Long, strand: Long, transcript_consequences: Option[Seq[TranscriptFull]])

case class OccurrenceMid(oid: String, donorId: String, vend: Long, projectId: String, vstart: Long, Reference_Allele: String, Tumor_Seq_Allele1: String, Tumor_Seq_Allele2: String, chromosome: String, allele_string: String, assembly_name: String, end: Long, vid: String, input: String, most_severe_consequence: String, seq_region_name: String, start: Long, strand: Long, transcript_consequences: Seq[TranscriptFull2])
