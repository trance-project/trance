
// Method used to hack the Mongo UUID object and only print out the UUID string
const generateUUID = () => {
    return UUID().toString().split('UUID(').join("").split(')').join("").split("\"").join("");
}
/*
    This is the init data when you would like to reset db state it should revert to this state
 */

/////////TRANCE_OBJECTS////////////////
const objects = [
    {
        _id: generateUUID(),
        name: "samples",
        abr:"s",
        columns:
            [
                {name: "bcr_patient_uuid", children:{}},
                {name: "bcr_sample_barcode", children:{}},
                {name: "bcr_aliquot_barcode", children:{}},
                {name: "bcr_aliquot_uuid", children:{}},
                {name: "biospecimen_barcode_bottom", children:{}},
                {name: "center_id", children:{}},
                {name: "concentration", children:{}},
                {name: "date_of_shipment", children:{}},
                {name: "is_derived_from_ffpe", children:{}},
                {name: "plate_column", children:{}},
                {name: "plate_id", children:{}},
                {name: "plate_row", children:{}},
                {name: "quantity", children:{}},
                {name: "source_center", children:{}},
                {name: "volume", children:{}},
            ]
    },
    {
        _id: generateUUID(),
        name: "copyNumber",
        abr:"c",
        columns:
            [
                {name: "cn_gene_id", children:{}},
                {name: "cn_gene_name", children:{}} ,
                {name: "cn_chromosome", children:{}},
                {name: "cn_start", children:{}},
                {name: "cn_gene_id", children:{}},
                {name: "cn_gene_name", children:{}},
                {name: "cn_chromosome", children:{}},
                {name: "cn_start", children:{}},
                {name: "cn_end", children:{}},
                {name: "cn_copy_number", children:{}},
                {name: "min_copy_number", children:{}},
                {name: "max_copy_number", children:{}},
                {name: "cn_aliquot_uuid", children:{}},
            ]
    },
    {
        _id: generateUUID(),
        name: "occurrences",
        abr:"o",
        columns:
            [
                {name: "oid", children:{}},
                {name: "donorId", children:{}} ,
                {name: "vend",  children:{}},
                {name: "projectId", children:{}},
                {name: "vstart", children:{}},
                {name: "Tumor_Seq_Allele1", children:{}},
                {name: "Tumor_Seq_Allele2", children:{}},
                {name: "chromosome", children:{}},
                {name: "allele_string", children:{}},
                {name: "assembly_name", children:{}},
                {name: "end", children:{}},
                {name: "vid", children:{}},
                {name: "input", children:{}},
                {name: "most_severe_consequence", children:{}},
                {name: "seq_region_name", children:{}},
                {name: "start", children:{}},
                {name: "strand", children:{}},
                {name: "transcript_consequences", children:{}},
            ]
    },
    {
        _id: generateUUID(),
        name: "transcript_consequences",
        abr:"t",
        columns:
            [
                {name: "gene_id", children:{}},
                {name: "impact", children:{}},
            ]
    },
    {
        _id: generateUUID(),
        name: "candidates",
        abr:"cc",
        columns:
            [
                {name: "case_id", children:{}},
                {name: "amino_acids", children:{}} ,
                {name: "cdna_end",  children:{}},
                {name: "cds_end", children:{}},
                {name: "cds_start", children:{}},
                {name: "codons", children:{}},
                {name: "consequence_terms", children:{}},
                {name: "distance", children:{}},
                {name: "exon", children:{}},                
                {name: "flags", children:{}},
                {name: "gene_id", children:{}},                
                {name: "impact", children:{}},
                {name: "impact2", children:{}},                
                {name: "intron", children:{}},
                {name: "polyphen_prediction", children:{}},                
                {name: "polyphen_score", children:{}},
                {name: "protein_end", children:{}},                
                {name: "protein_start", children:{}},
                {name: "sift_prediction", children:{}},
                {name: "sift_score", children:{}},                
                {name: "ts_strand", children:{}},
                {name: "transcript_id", children:{}},
                {name: "variant_allele", children:{}},
            ]
    },
    {
        _id: generateUUID(),
        name: "consequences",
        abr:"cs",
        columns:
            [
                {name: "element", children:{}},
            ]
    },
    {
        _id: generateUUID(),
        name: "geneExpression",
        abr:"ge",
        columns:
            [
                {name: "ge_aliquot", children:{}},
                {name: "ge_gene_id", children:{}},
                {name: "ge_fpkm", children:{}},
            ]
    },
    {
        _id: generateUUID(),
        name: "proteinInteractions",
        abr:"pi",
        columns:
            [
                {name: "id", children:{}},
                {name: "odate", children:{}},
                {name: "oparts", children:{}},
                {name: "odescription", children:{}},
                {name: "oclerk", children:{}},
            ]
    },
    {
        _id: generateUUID(),
        name: "drugResponse",
        abr:"dr",
        columns:
            [
                {name: "id", children:{}},
                {name: "nname", children:{}},
                {name: "ncusts", children:{}},
                {name: "ncountry", children:{}},
            ]
    },
    {
        _id: generateUUID(),
        name: "enhancers",
        abr:"e",
        columns:
            [
                {name: "id", children:{}},
                {name: "rname", children:{}},
                {name: "rnation", children:{}},
            ]
    },
    {
        _id: generateUUID(),
        name: "methylation",
        abr:"m",
        columns:
            [
                {name: "id", children:{}},
                {name: "qty", children:{}},
                {name: "orderid", children:{}},
                {name: "price", children:{}},
                {name: "deliveroption", children:{}},
            ]
    },
    {
        _id: generateUUID(),
        name: "genes",
        abr:"g",
        columns:
            [
                {name: "id", children:{}},
                {name: "pname", children:{}},
                {name: "pdescription", children:{}},
                {name: "pmanufacturer", children:{}},
            ]
    },
    {
        _id: generateUUID(),
        name: "pathways",
        abr:"pw",
        columns:
            [
                {name: "id", children:{}},
                {name: "qty", children:{}},
                {name: "price", children:{}},
                {name: "description", children:{}},
            ]
    },
    {
        _id: generateUUID(),
        name: "variants",
        abr:"v",
        columns:
            [
                {name: "id", children:{}},
                {name: "sname", children:{}},
                {name: "sdescription", children:{}},
                {name: "sprice", children:{}},
            ]
    },
    {
        _id: generateUUID(),
        name: "COP",
        abr:"cop",
        columns:
            [
                {name: "cname", children:{}},
                {name: "corders", children:{}},
            ]
    }, 
    {
        _id: generateUUID(),
        name: "corders",
        abr:"co",
        columns:
            [
                {name: "odate", children:{}},
                {name: "oparts", children:{}},
            ]
    },   
    {
        _id: generateUUID(),
        name: "oparts",
        abr:"op",
        columns:
            [
                {name: "pid", children:{}},
                {name: "qty", children:{}},
            ]
    },   
    {
        _id: generateUUID(),
        name: "part",
        abr:"p",
        columns:
            [
                {name: "pid", children:{}},
                {name: "pname", children:{}},
                {name: "price", children:{}},
            ]
    }        
]
 const queryObject = [{
     _id: generateUUID(),
    title: "Test",
     body: "1234wjew"
 }]

///////////// END INIT DATA ///////////////////

print('Start #################################');

db = db.getSiblingDB('api_prod_db');
db.createUser(
    {
        user: 'trance_api_user',
        pwd: 'trance1234', // todo secure password
        roles: [{role: 'readWrite', db: 'api_prod_db'}]
    }
);
db.createCollection('users');

db = db.getSiblingDB('api_dev_db');
db.createUser(
    {
        user: 'trance_api_user',
        pwd: 'trance1234', // todo secure password
        roles: [{role: 'readWrite', db: 'api_dev_db'}]
    }
);
db.createCollection('trance_objects');
db.createCollection('query');
db.createCollection('blockly_document');
print("############# Start Insert Init Data##############")

db.trance_objects.insertMany(objects)
db.query.insertMany(queryObject)
print("############# End Insert Init Data##############")

db = db.getSiblingDB('api_test_db');
db.createUser(
    {
        user: 'trance_api_user',
        pwd: 'trance1234', // todo secure password
        roles: [{role: 'readWrite', db: 'api_test_db'}]
    }
);
db.createCollection('users');

print('END #################################');