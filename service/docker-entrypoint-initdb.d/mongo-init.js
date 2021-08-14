
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
        isCollectionType: false,
        abr:"s",
        columns:
            [
                {name: "bcr_patient_uuid", dataType: "String"},
                {name: "bcr_sample_barcode", dataType: "String"},
                {name: "bcr_aliquot_barcode", dataType: "String"},
                {name: "bcr_aliquot_uuid", dataType: "String"},
                {name: "biospecimen_barcode_bottom", dataType: "String"},
                {name: "center_id", dataType: "String"},
                {name: "concentration", dataType: "String"},
                {name: "date_of_shipment", dataType: "String"},
                {name: "is_derived_from_ffpe", dataType: "String"},
                {name: "plate_column", dataType: "String"},
                {name: "plate_id", dataType: "String"},
                {name: "plate_row", dataType: "String"},
                {name: "quantity", dataType: "String"},
                {name: "source_center", dataType: "String"},
                {name: "volume", dataType: "String"},
            ]
    },
    {
        _id: generateUUID(),
        name: "copyNumber",
        isCollectionType: false,
        abr:"c",
        columns:
            [
                {name: "cn_gene_id", dataType: "String"},
                {name: "cn_gene_name", dataType: "String"} ,
                {name: "cn_chromosome", dataType: "String"},
                {name: "cn_start", dataType: "String"},
                {name: "cn_gene_id", dataType: "String"},
                {name: "cn_gene_name", dataType: "String"},
                {name: "cn_chromosome", dataType: "String"},
                {name: "cn_start", dataType: "String"},
                {name: "cn_end", dataType: "String"},
                {name: "cn_copy_number", dataType: "String"},
                {name: "min_copy_number", dataType: "String"},
                {name: "max_copy_number", dataType: "String"},
                {name: "cn_aliquot_uuid", dataType: "String"},
            ]
    },
    {
        _id: generateUUID(),
        name: "occurrences",
        isCollectionType: false,
        abr:"o",
        columns:
            [
                {name: "oid", dataType: "String"},
                {name: "donorId", dataType: "String"} ,
                {name: "vend",  dataType: "String"},
                {name: "projectId", dataType: "String"},
                {name: "vstart", dataType: "String"},
                {name: "Tumor_Seq_Allele1", dataType: "String"},
                {name: "Tumor_Seq_Allele2", dataType: "String"},
                {name: "chromosome", dataType: "String"},
                {name: "allele_string", dataType: "String"},
                {name: "assembly_name", dataType: "String"},
                {name: "end", dataType: "String"},
                {name: "vid", dataType: "String"},
                {name: "input", dataType: "String"},
                {name: "most_severe_consequence", dataType: "String"},
                {name: "seq_region_name", dataType: "String"},
                {name: "start", dataType: "String"},
                {name: "strand", dataType: "String"},
                {name: "transcript_consequences", dataType: "Collection"},
            ]
    },
    {
        _id: generateUUID(),
        name: "transcript_consequences",
        isCollectionType: true,
        abr:"t",
        columns:
            [
                {name: "gene_id", dataType: "String"},
                {name: "impact", dataType: "String"},
            ]
    },
    {
        _id: generateUUID(),
        name: "candidates",
        isCollectionType: false,
        abr:"cc",
        columns:
            [
                {name: "case_id", dataType: "String"},
                {name: "amino_acids", dataType: "String"} ,
                {name: "cdna_end",  dataType: "String"},
                {name: "cds_end", dataType: "String"},
                {name: "cds_start", dataType: "String"},
                {name: "codons", dataType: "String"},
                {name: "consequence_terms", dataType: "String"},
                {name: "distance", dataType: "String"},
                {name: "exon", dataType: "String"},                
                {name: "flags", dataType: "String"},
                {name: "gene_id", dataType: "String"},                
                {name: "impact", dataType: "String"},
                {name: "impact2", dataType: "String"},                
                {name: "intron", dataType: "String"},
                {name: "polyphen_prediction", dataType: "String"},                
                {name: "polyphen_score", dataType: "String"},
                {name: "protein_end", dataType: "String"},                
                {name: "protein_start", dataType: "String"},
                {name: "sift_prediction", dataType: "String"},
                {name: "sift_score", dataType: "String"},                
                {name: "ts_strand", dataType: "String"},
                {name: "transcript_id", dataType: "String"},
                {name: "variant_allele", dataType: "String"},
            ]
    },
    {
        _id: generateUUID(),
        name: "consequences",
        isCollectionType: false,
        abr:"cs",
        columns:
            [
                {name: "element", dataType: "String"},
            ]
    },
    {
        _id: generateUUID(),
        name: "geneExpression",
        isCollectionType: false,
        abr:"ge",
        columns:
            [
                {name: "ge_aliquot", dataType: "String"},
                {name: "ge_gene_id", dataType: "String"},
                {name: "ge_fpkm", dataType: "String"},
            ]
    },
    {
        _id: generateUUID(),
        name: "proteinInteractions",
        isCollectionType: false,
        abr:"pi",
        columns:
            [
                {name: "id", dataType: "String"},
                {name: "odate", dataType: "String"},
                {name: "oparts", dataType: "String"},
                {name: "odescription", dataType: "String"},
                {name: "oclerk", dataType: "String"},
            ]
    },
    {
        _id: generateUUID(),
        name: "drugResponse",
        isCollectionType: false,
        abr:"dr",
        columns:
            [
                {name: "id", dataType: "String"},
                {name: "nname", dataType: "String"},
                {name: "ncusts", dataType: "String"},
                {name: "ncountry", dataType: "String"},
            ]
    },
    {
        _id: generateUUID(),
        name: "enhancers",
        isCollectionType: false,
        abr:"e",
        columns:
            [
                {name: "id", dataType: "String"},
                {name: "rname", dataType: "String"},
                {name: "rnation", dataType: "String"},
            ]
    },
    {
        _id: generateUUID(),
        name: "methylation",
        isCollectionType: false,
        abr:"m",
        columns:
            [
                {name: "id", dataType: "String"},
                {name: "qty", dataType: "String"},
                {name: "orderid", dataType: "String"},
                {name: "price", dataType: "String"},
                {name: "deliveroption", dataType: "String"},
            ]
    },
    {
        _id: generateUUID(),
        name: "genes",
        isCollectionType: false,
        abr:"g",
        columns:
            [
                {name: "id", dataType: "String"},
                {name: "pname", dataType: "String"},
                {name: "pdescription", dataType: "String"},
                {name: "pmanufacturer", dataType: "String"},
            ]
    },
    {
        _id: generateUUID(),
        name: "pathways",
        isCollectionType: false,
        abr:"pw",
        columns:
            [
                {name: "id", dataType: "String"},
                {name: "qty", dataType: "String"},
                {name: "price", dataType: "String"},
                {name: "description", dataType: "String"},
            ]
    },
    {
        _id: generateUUID(),
        name: "variants",
        isCollectionType: false,
        abr:"v",
        columns:
            [
                {name: "id", dataType: "String"},
                {name: "sname", dataType: "String"},
                {name: "sdescription", dataType: "String"},
                {name: "sprice", dataType: "String"},
            ]
    },
    {
        _id: generateUUID(),
        name: "COP",
        abr:"cop",
        isCollectionType: false,
        columns:
            [
                {name: "cname", dataType: "String"},
                {name: "corders", dataType: "String"},
            ]
    }, 
    {
        _id: generateUUID(),
        name: "corders",
        abr:"co",
        isCollectionType: false,
        columns:
            [
                {name: "odate", dataType: "String"},
                {name: "oparts", dataType: "String"},
            ]
    },   
    {
        _id: generateUUID(),
        name: "oparts",
        isCollectionType: false,
        abr:"op",
        columns:
            [
                {name: "pid", dataType: "String"},
                {name: "qty", dataType: "String"},
            ]
    },   
    {
        _id: generateUUID(),
        name: "part",
        isCollectionType: false,
        abr:"p",
        columns:
            [
                {name: "pid", dataType: "String"},
                {name: "pname", dataType: "String"},
                {name: "price", dataType: "String"},
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