"""Stream-analysis tools. Names are exported lazily (PEP 562): importing this package, or a
light submodule such as `corruption`, does not import the heavy optional dependencies
(torch, transformers, hdbscan, umap, cv2, ipywidgets, anthropic, ...). A name is resolved, and
its submodule imported, on first access, so `from archival_structures.stream_analysis import X`
works as before; X's submodule then needs whichever extras it requires.
"""

from importlib import import_module
from typing import TYPE_CHECKING

_LAZY_EXPORTS = {
    # config
    "AnalysisConfig": "archival_structures.stream_analysis.config",
    # corruption
    "CASE_NAMES": "archival_structures.stream_analysis.corruption",
    "CorruptedStream": "archival_structures.stream_analysis.corruption",
    "PRF1": "archival_structures.stream_analysis.corruption",
    "SimulationResult": "archival_structures.stream_analysis.corruption",
    "document_level_swap": "archival_structures.stream_analysis.corruption",
    "extract_canonical_stream": "archival_structures.stream_analysis.corruption",
    "foreign_document_insertion": "archival_structures.stream_analysis.corruption",
    "grouping_ari": "archival_structures.stream_analysis.corruption",
    "initial_provenance": "archival_structures.stream_analysis.corruption",
    "intra_document_swap": "archival_structures.stream_analysis.corruption",
    "observed_start_labels": "archival_structures.stream_analysis.corruption",
    "pairs_to_groups": "archival_structures.stream_analysis.corruption",
    "read_corrupted_stream": "archival_structures.stream_analysis.corruption",
    "score_same_document_pairs": "archival_structures.stream_analysis.corruption",
    "score_start_classification": "archival_structures.stream_analysis.corruption",
    "simulate_collection": "archival_structures.stream_analysis.corruption",
    "simulate_inventory": "archival_structures.stream_analysis.corruption",
    "swap_and_insertion": "archival_structures.stream_analysis.corruption",
    "true_same_document_pairs": "archival_structures.stream_analysis.corruption",
    "true_start_indices": "archival_structures.stream_analysis.corruption",
    "window_diff": "archival_structures.stream_analysis.corruption",
    "write_corrupted_stream": "archival_structures.stream_analysis.corruption",
    "write_manifest": "archival_structures.stream_analysis.corruption",
    # sequence_analysis
    "run_length_encode": "archival_structures.stream_analysis.sequence_analysis",
    "merge_short_runs": "archival_structures.stream_analysis.sequence_analysis",
    "ngram_counts": "archival_structures.stream_analysis.sequence_analysis",
    "find_frequent_ngrams": "archival_structures.stream_analysis.sequence_analysis",
    "find_tandem_repeats": "archival_structures.stream_analysis.sequence_analysis",
    "label_transition_matrix": "archival_structures.stream_analysis.sequence_analysis",
    "coarsen_by_hierarchy": "archival_structures.stream_analysis.sequence_analysis",
    # overview.subsequence_detection
    "compute_adjacent_similarities": "archival_structures.stream_analysis.overview.subsequence_detection",
    "suggest_threshold": "archival_structures.stream_analysis.overview.subsequence_detection",
    "detect_boundaries_threshold": "archival_structures.stream_analysis.overview.subsequence_detection",
    "detect_boundaries_changepoint": "archival_structures.stream_analysis.overview.subsequence_detection",
    "boundaries_to_segments": "archival_structures.stream_analysis.overview.subsequence_detection",
    "score_segment": "archival_structures.stream_analysis.overview.subsequence_detection",
    "score_all_segments": "archival_structures.stream_analysis.overview.subsequence_detection",
    "detect_book_like_subsequences": "archival_structures.stream_analysis.overview.subsequence_detection",
    "MAX_SEGMENT_LENGTH": "archival_structures.stream_analysis.overview.subsequence_detection",
    "segment_dp": "archival_structures.stream_analysis.overview.subsequence_detection",
    "calibrate_length_penalty_weight": "archival_structures.stream_analysis.overview.subsequence_detection",
    "textiling_depth_scores": "archival_structures.stream_analysis.overview.subsequence_detection",
    "predicted_start_rate": "archival_structures.stream_analysis.overview.subsequence_detection",
    "starts_to_boundaries": "archival_structures.stream_analysis.overview.subsequence_detection",
    "boundaries_to_starts": "archival_structures.stream_analysis.overview.subsequence_detection",
    # overview.pipeline
    "run_overview": "archival_structures.stream_analysis.overview.pipeline",
    "cluster_inventories": "archival_structures.stream_analysis.overview.pipeline",
    "load_combined_inventories": "archival_structures.stream_analysis.overview.pipeline",
    # overview.clustering
    "reduce_dimensions": "archival_structures.stream_analysis.overview.clustering",
    "cluster_embeddings": "archival_structures.stream_analysis.overview.clustering",
    "get_cluster_members": "archival_structures.stream_analysis.overview.clustering",
    "save_cluster_hierarchy": "archival_structures.stream_analysis.overview.clustering",
    "load_cluster_hierarchy": "archival_structures.stream_analysis.overview.clustering",
    "cluster_label_map": "archival_structures.stream_analysis.overview.clustering",
    # overview.cluster_neighbourhood
    "get_cluster_tree_maps": "archival_structures.stream_analysis.overview.cluster_neighbourhood",
    "nearest_cluster_branches": "archival_structures.stream_analysis.overview.cluster_neighbourhood",
    "cluster_centroids": "archival_structures.stream_analysis.overview.cluster_neighbourhood",
    "rank_by_centroid_distance": "archival_structures.stream_analysis.overview.cluster_neighbourhood",
    "describe_cluster_neighbourhood": "archival_structures.stream_analysis.overview.cluster_neighbourhood",
    "format_cluster_neighbourhood": "archival_structures.stream_analysis.overview.cluster_neighbourhood",
    # groundtruth.pipeline
    "run_export": "archival_structures.stream_analysis.groundtruth.pipeline",
    "run_active_learning": "archival_structures.stream_analysis.groundtruth.pipeline",
    # groundtruth.interactive_annotation
    "annotate_image_grid": "archival_structures.stream_analysis.groundtruth.interactive_annotation",
    "annotate_cluster": "archival_structures.stream_analysis.groundtruth.interactive_annotation",
    "load_labels": "archival_structures.stream_analysis.groundtruth.interactive_annotation",
    "save_labels": "archival_structures.stream_analysis.groundtruth.interactive_annotation",
    # groundtruth.boundary_annotation
    "annotate_page_boundaries": "archival_structures.stream_analysis.groundtruth.boundary_annotation",
    "load_boundary_labels": "archival_structures.stream_analysis.groundtruth.boundary_annotation",
    "save_boundary_labels": "archival_structures.stream_analysis.groundtruth.boundary_annotation",
    "to_ground_truth_pairs": "archival_structures.stream_analysis.groundtruth.boundary_annotation",
    "to_inventory_ground_truth": "archival_structures.stream_analysis.groundtruth.boundary_annotation",
}

if TYPE_CHECKING:  # static analysers and IDEs see the real imports
    from archival_structures.stream_analysis.config import (
        AnalysisConfig,
    )
    from archival_structures.stream_analysis.corruption import (
        CASE_NAMES,
        CorruptedStream,
        PRF1,
        SimulationResult,
        document_level_swap,
        extract_canonical_stream,
        foreign_document_insertion,
        grouping_ari,
        initial_provenance,
        intra_document_swap,
        observed_start_labels,
        pairs_to_groups,
        read_corrupted_stream,
        score_same_document_pairs,
        score_start_classification,
        simulate_collection,
        simulate_inventory,
        swap_and_insertion,
        true_same_document_pairs,
        true_start_indices,
        window_diff,
        write_corrupted_stream,
        write_manifest,
    )
    from archival_structures.stream_analysis.sequence_analysis import (
        run_length_encode,
        merge_short_runs,
        ngram_counts,
        find_frequent_ngrams,
        find_tandem_repeats,
        label_transition_matrix,
        coarsen_by_hierarchy,
    )
    from archival_structures.stream_analysis.overview.subsequence_detection import (
        compute_adjacent_similarities,
        suggest_threshold,
        detect_boundaries_threshold,
        detect_boundaries_changepoint,
        boundaries_to_segments,
        score_segment,
        score_all_segments,
        detect_book_like_subsequences,
    )
    from archival_structures.stream_analysis.overview.pipeline import (
        run_overview,
        cluster_inventories,
        load_combined_inventories,
    )
    from archival_structures.stream_analysis.overview.clustering import (
        reduce_dimensions,
        cluster_embeddings,
        get_cluster_members,
        save_cluster_hierarchy,
        load_cluster_hierarchy,
        cluster_label_map,
    )
    from archival_structures.stream_analysis.overview.cluster_neighbourhood import (
        get_cluster_tree_maps,
        nearest_cluster_branches,
        cluster_centroids,
        rank_by_centroid_distance,
        describe_cluster_neighbourhood,
        format_cluster_neighbourhood,
    )
    from archival_structures.stream_analysis.groundtruth.pipeline import (
        run_export,
        run_active_learning,
    )
    from archival_structures.stream_analysis.groundtruth.interactive_annotation import (
        annotate_image_grid,
        annotate_cluster,
        load_labels,
        save_labels,
    )
    from archival_structures.stream_analysis.groundtruth.boundary_annotation import (
        annotate_page_boundaries,
        load_boundary_labels,
        save_boundary_labels,
        to_ground_truth_pairs,
        to_inventory_ground_truth,
    )


__all__ = [
    "AnalysisConfig",
    "CASE_NAMES",
    "CorruptedStream",
    "PRF1",
    "SimulationResult",
    "document_level_swap",
    "extract_canonical_stream",
    "foreign_document_insertion",
    "grouping_ari",
    "initial_provenance",
    "intra_document_swap",
    "observed_start_labels",
    "pairs_to_groups",
    "read_corrupted_stream",
    "score_same_document_pairs",
    "score_start_classification",
    "simulate_collection",
    "simulate_inventory",
    "swap_and_insertion",
    "true_same_document_pairs",
    "true_start_indices",
    "window_diff",
    "write_corrupted_stream",
    "write_manifest",
    "run_length_encode",
    "merge_short_runs",
    "ngram_counts",
    "find_frequent_ngrams",
    "find_tandem_repeats",
    "label_transition_matrix",
    "coarsen_by_hierarchy",
    "compute_adjacent_similarities",
    "suggest_threshold",
    "detect_boundaries_threshold",
    "detect_boundaries_changepoint",
    "boundaries_to_segments",
    "score_segment",
    "score_all_segments",
    "detect_book_like_subsequences",
    "run_overview",
    "cluster_inventories",
    "load_combined_inventories",
    "reduce_dimensions",
    "cluster_embeddings",
    "get_cluster_members",
    "save_cluster_hierarchy",
    "load_cluster_hierarchy",
    "cluster_label_map",
    "get_cluster_tree_maps",
    "nearest_cluster_branches",
    "cluster_centroids",
    "rank_by_centroid_distance",
    "describe_cluster_neighbourhood",
    "format_cluster_neighbourhood",
    "run_export",
    "run_active_learning",
    "annotate_image_grid",
    "annotate_cluster",
    "load_labels",
    "save_labels",
    "annotate_page_boundaries",
    "load_boundary_labels",
    "save_boundary_labels",
    "to_ground_truth_pairs",
    "to_inventory_ground_truth",
]


def __getattr__(name):
    try:
        module_name = _LAZY_EXPORTS[name]
    except KeyError:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}") from None
    value = getattr(import_module(module_name), name)
    globals()[name] = value  # cache so __getattr__ runs once per name
    return value


def __dir__():
    return sorted(set(globals()) | set(__all__))
