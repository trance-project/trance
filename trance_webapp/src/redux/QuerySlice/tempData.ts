import {QuerySummaryList, Query, Table, Association, NewQuery} from "../../utils/Public_Interfaces";

/**
 * this is temp data should be removed when rest web service is active
 * @param id
 * @param date
 * @param name
 * @param tables
 * @param groupedBy
 */
const createData = (id: string, date: string, name:string, tables: string, groupedBy: string) => ({id, date, name, tables, groupedBy, });

/**
 * @Tables Data
 */
const sampleTable = {name: "Sample",
    id: 1,
    abr:"s",
    columns:
        [
            {id: 111,tableAssociation:'s',name: "sample", enable: true},
            {id: 112,tableAssociation:'s',name: "turmorsite", enable: false},
            {id: 133,tableAssociation:'s',name: "treatment_outcome", enable: false},
        ]
} as Table

const CopyNumberTable = {name: "CopyNumber",
    id: 113,
    abr:"c",
    columns:
        [
            {id: 114,tableAssociation:'c',name: "sample", enable: false},
            {id: 115,tableAssociation:'c',name: "gene", enable: true} ,
            {id: 116,tableAssociation:'c',name: "cnum", enable: false},
        ]
} as Table

const OccurrencesTable = {name: "Occurrences",
    id: 117,
    abr:"o",
    columns:
        [
            {id: 118,tableAssociation:'o',name: "sample", enable: false},
            {id: 119,tableAssociation:'o',name: "contig", enable: false} ,
            {id: 120,tableAssociation:'o',name: "end", enable: false},
            {id: 121,tableAssociation:'o',name: "reference", enable: false},
            {id: 122,tableAssociation:'o',name: "alternate", enable: false},
            {id: 123,tableAssociation:'o',name: "mutid", enable: true},
            {id: 124,tableAssociation:'o',name: "candidates", enable: false, children:
                    {name: "candidates",
                        id: 125,
                        abr: 't',
                        columns:
                            [
                                {id: 126,tableAssociation:'t',name: "gene", enable: true},
                                {id: 127,tableAssociation:'t',name: "impact", enable: true},
                                {id: 128,tableAssociation:'t',name: "sift", enable: true},
                                {id: 129,tableAssociation:'t',name: "poly", enable: true},
                                {id: 130,tableAssociation:'t',name: "consequences", enable: true, children:
                                        {name: "consequences",
                                            abr:'f',
                                            id: 131,
                                            columns:
                                                [
                                                    {id: 132,tableAssociation:'f',name: "conseq", enable: true}
                                                ]
                                        }
                                    },
                            ]
                    }
                }
                ]
} as Table

/**
 * @Associations
 */
const sample_Occurrences_Associations = {
    key: "mutations",
    association: [{
      objectAssociation: [{id: 114,tableAssociation:'c',name: "sample", enable: false}, {id: 118,tableAssociation:'o',name: "sample", enable: false}],
        objects: [sampleTable, OccurrencesTable]
    }]
} as Association;

const Occurrences_CopyNumber_Associations = {
    key: "Scores",
    association: [
        {
        objectAssociation: [{id: 124,tableAssociation:'o',name: "candidates", enable: false, children:
                {name: "candidates",
                    id: 125,
                    abr: 't',
                    columns:
                        [
                            {id: 126,tableAssociation:'t',name: "gene", enable: true},
                        ]
                }
            }, {id: 115,tableAssociation:'c',name: "gene", enable: true} ],
            objects: [OccurrencesTable, CopyNumberTable]
        },
        {
            objectAssociation: [{id: 114,tableAssociation:'o',name: "sample", enable: false}, {id: 114,tableAssociation:'c',name: "sample", enable: false}],
            objects: [OccurrencesTable, CopyNumberTable]
        }]

} as Association;

/**
 * @Queries&SubQueries
 */
const ThirdLevelQuerySelected = {
    name: "demo_query_3",
    level: "3",
    type:"select",
    table:  CopyNumberTable,
    filters: ["score := t.impact * (c.cnum + 0.01) * t.sift * t.poly"],
    groupBy: {key: "score_gene", type: "sumBy"}
} as Query;

const secondLevelQuerySelected = {
    name: "demo_query_2",
    level: "2",
    type:"select",
    table:  OccurrencesTable,
    children:ThirdLevelQuerySelected,
    associations:[Occurrences_CopyNumber_Associations]
} as Query;

export const rows: QuerySummaryList[] = [
    createData("0", '12 Mar, 2021', 'GeneLikelihoodPerMutation', 'Samples,Occurrences,CopyNumber', 'All'),
    createData("1", '12 Mar, 2021', 'GeneLikelihoodPerSample', 'Samples,GeneLikelihoodPerMutation', 'Shredded'),
    createData("2", '12 Mar, 2021', 'GeneImpactPerMutation', 'Samples,Occurrences', 'Standard'),
    createData("3", '12 Mar, 2021', 'GeneImpactPerSample', 'Samples,Occurrences', 'All'),
    createData("4", '12 Mar, 2021', 'HybridScoreMatrix', 'Occurrences,CopyNumber', 'Shredded'),
    createData("5", '11 Mar, 2021', 'EffectScoreMatrix', 'HyrbidScoreMatrix,Network', 'Shredded'),
    createData("Demo_query_6", '11 Mar, 2021', 'NetworkEffects', 'HybridScoreMatrix,GeneExpression', 'All')
];

export const querySelected = {
    name: "Demo_query_6",
    level: "1",
    type:"select",
    table:  sampleTable,
    children:secondLevelQuerySelected,
    associations: [sample_Occurrences_Associations]
} as Query

export const newQuerySelected =  {
    name: "QuerySimple",
    key: "For s in samples Union ",
    labels: [{
        name: "sample",
        key: "s.bcr_patient_uuid"
        }, {
        name: "mutations",
        key: "For o in occurrences Union If (s.bcr_patient_uuid = o.donorId) Then ",
        labels: [{
            name: "mutId",
            key: "o.oid",
        }, {
            name : "scores",
            key : "ReduceByKey[gene], [score], For t in o.transcript_consequences Union For c in copynumber Union If (t.gene_id = c.cn_gene_id AND c.cn_aliquot_uuid = s.bcr_aliquot_uuid) Then ",
            labels : [{
                name: "gene",
                key : "t.gene_id"
            }, {
                name : "score",
                key : "((c.cn_copy_number + 0.01) * If (t.impact = HIGH) Then 0.8 Else If (t.impact = MODERATE) Then 0.5 Else If (t.impact = LOW) Then 0.3 Else 0.01   )"
            } ]
        }]
    }]
}  as NewQuery





