 
include { FAMSA_PARTTREE } from '../../modules/local/famsa_parttree'
include { CLUSTALO_MBEDTREE } from '../../modules/local/clustalo_mbedtree'

 
 workflow COMPUTE_TREES {

    take:
    ch_fastas               //channel: [ meta, /path/to/file.fasta ]
    tree_tools              //channel: [ meta ] ( tools to be run: meta.tree, meta.args_tree )
     
    main:
    ch_versions = Channel.empty()

    // 
    // Render the required guide trees  
    //

   // Branch each guide tree rendering into a separate channel
   ch_fastas_fortrees = ch_fastas.combine(tree_tools)
                                 .map( it -> [it[0] + it[2], it[1]] )
                                 .branch{
                                          parttree: it[0]["tree"] ==  "PARTTREE"
                                          mbed: it[0]["tree"] == "MBED"
                                        }


    FAMSA_PARTTREE(ch_fastas_fortrees.parttree)
    ch_trees = FAMSA_PARTTREE.out.tree
    ch_versions = ch_versions.mix(FAMSA_PARTTREE.out.versions.first())


    CLUSTALO_MBEDTREE(ch_fastas_fortrees.mbed)
    ch_trees = ch_trees.mix(CLUSTALO_MBEDTREE.out.tree)
    ch_versions = ch_versions.mix(CLUSTALO_MBEDTREE.out.versions.first())


    emit:
    trees            = ch_trees                  // channel: [ val(meta), path(tree) ]             
    versions         = ch_versions.ifEmpty(null) // channel: [ versions.yml ]

 }
 
 
 
