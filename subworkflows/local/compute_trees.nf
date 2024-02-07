
include { FAMSA_GUIDETREE } from '../../modules/nf-core/famsa/guidetree/main'
include { CLUSTALO_GUIDETREE } from '../../modules/nf-core/clustalo/guidetree/main'


workflow COMPUTE_TREES {

    take:
    ch_fastas               //channel: [ meta, /path/to/file.fasta ]
    tree_tools              //channel: [ meta ] ( tools to be run: meta.tree, meta.args_tree )

    main:
    ch_versions = Channel.empty()

    //
    // Render the required guide trees
    //
    ch_fastas_fortrees = ch_fastas
                            .combine(tree_tools)
                            .map{ metafasta, fasta, metatree -> [metafasta+metatree, fasta] }
                            .branch{
                                famsa:    it[0]["tree"] == "FAMSA"
                                clustalo: it[0]["tree"] == "CLUSTALO"
                            }

    FAMSA_GUIDETREE(ch_fastas_fortrees.famsa)
    ch_trees = FAMSA_GUIDETREE.out.tree
    ch_versions = ch_versions.mix(FAMSA_GUIDETREE.out.versions.first())

    CLUSTALO_GUIDETREE(ch_fastas_fortrees.clustalo)
    ch_trees = ch_trees.mix(CLUSTALO_GUIDETREE.out.tree)
    ch_versions = ch_versions.mix(CLUSTALO_GUIDETREE.out.versions.first())

    emit:
    trees            = ch_trees                  // channel: [ val(meta), path(tree) ]
    versions         = ch_versions.ifEmpty(null) // channel: [ versions.yml ]
}



