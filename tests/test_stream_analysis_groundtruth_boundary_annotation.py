import pytest
from PIL import Image

from archival_structures.stream_analysis.groundtruth.boundary_annotation import (
    annotate_page_boundaries,
    compute_window,
    load_boundary_labels,
    next_unmarked,
    save_boundary_labels,
    to_ground_truth_pairs,
    to_inventory_ground_truth,
)


class TestComputeWindow:
    def test_clamps_at_start(self):
        assert compute_window(0, 20, 9) == (0, 9)

    def test_clamps_at_end(self):
        # n=20 images -> valid gap cursors are 0..18; the last gap should still show a
        # full-width window ending at the last image
        assert compute_window(18, 20, 9) == (11, 20)

    def test_centres_in_the_middle(self):
        win_start, win_end = compute_window(10, 20, 9)
        assert win_end - win_start == 9
        assert win_start <= 10 < win_end - 1  # the cursor's left-hand image is inside the window

    def test_window_larger_than_sequence_shows_everything(self):
        assert compute_window(1, 3, 9) == (0, 3)

    def test_window_size_floor_is_two(self):
        # a window smaller than 2 thumbnails can't show any gap at all
        win_start, win_end = compute_window(0, 20, 1)
        assert win_end - win_start == 2


class TestNextUnmarked:
    def test_finds_next_unmarked_forward(self):
        assert next_unmarked([True, None, False, None, True], after=0) == 1

    def test_wraps_around(self):
        assert next_unmarked([None, True, True], after=1) == 0

    def test_none_when_fully_marked(self):
        assert next_unmarked([True, False, True], after=1) is None


class TestLoadSaveBoundaryLabels:
    def test_fresh_file_is_all_unmarked(self, tmp_path):
        image_ids = ['a.jpg', 'b.jpg', 'c.jpg']
        labels = load_boundary_labels(image_ids, tmp_path / 'labels.json')
        assert labels == [None, None]

    def test_round_trips(self, tmp_path):
        image_ids = ['a.jpg', 'b.jpg', 'c.jpg', 'd.jpg']
        label_file = tmp_path / 'labels.json'
        labels = load_boundary_labels(image_ids, label_file)
        labels[0] = True
        labels[1] = False
        save_boundary_labels(image_ids, labels, label_file)

        reloaded = load_boundary_labels(image_ids, label_file)
        assert reloaded == [True, False, None]

    def test_raises_on_image_ids_mismatch(self, tmp_path):
        label_file = tmp_path / 'labels.json'
        save_boundary_labels(['a.jpg', 'b.jpg'], [True], label_file)
        with pytest.raises(ValueError):
            load_boundary_labels(['x.jpg', 'y.jpg', 'z.jpg'], label_file)


class TestToGroundTruthPairs:
    def test_returns_the_list_when_complete(self):
        assert to_ground_truth_pairs([True, False, True]) == [True, False, True]

    def test_raises_when_incomplete(self):
        with pytest.raises(ValueError, match='1 of 3'):
            to_ground_truth_pairs([True, None, True])


class TestToInventoryGroundTruth:
    def test_groups_by_boundaries(self):
        image_ids = ['a.jpg', 'b.jpg', 'c.jpg', 'd.jpg']
        # a-b same, b-c boundary, c-d same -> two documents: {a,b} and {c,d}
        same_document = [True, False, True]
        gt = to_inventory_ground_truth(
            image_ids, same_document, 'TEST_1', {'unitid': 'TEST', 'file_unitid': '1'},
        )
        assert gt.inventory_id == 'TEST_1'
        documents = gt.layers['document']
        assert [s.scan_id for s in documents[0].spans] == ['a.jpg', 'b.jpg']
        assert [s.scan_id for s in documents[1].spans] == ['c.jpg', 'd.jpg']
        assert all(s.page_side == 'full' for el in documents for s in el.spans)

    def test_all_same_document_is_one_group(self):
        image_ids = ['a.jpg', 'b.jpg', 'c.jpg']
        gt = to_inventory_ground_truth(
            image_ids, [True, True], 'TEST_1', {'unitid': 'TEST', 'file_unitid': '1'},
        )
        assert len(gt.layers['document']) == 1
        assert [s.scan_id for s in gt.layers['document'][0].spans] == image_ids

    def test_every_boundary_is_n_singleton_groups(self):
        image_ids = ['a.jpg', 'b.jpg', 'c.jpg']
        gt = to_inventory_ground_truth(
            image_ids, [False, False], 'TEST_1', {'unitid': 'TEST', 'file_unitid': '1'},
        )
        assert len(gt.layers['document']) == 3

    def test_raises_when_annotation_incomplete(self):
        with pytest.raises(ValueError):
            to_inventory_ground_truth(
                ['a.jpg', 'b.jpg', 'c.jpg'], [True, None],
                'TEST_1', {'unitid': 'TEST', 'file_unitid': '1'},
            )


class TestAnnotatePageBoundariesConstruction:
    """Headless smoke tests: confirm the widget builds and runs its render logic without
    error. Genuine interaction (clicks, keypresses) needs a live browser and isn't exercised
    here -- consistent with the rest of this package's ipywidgets tools (e.g.
    `interactive_annotation.annotate_image_grid`), which have no interaction tests either.
    """

    @pytest.fixture
    def tiny_images(self, tmp_path):
        paths = []
        for i in range(6):
            path = tmp_path / f'page_{i:02d}.jpg'
            Image.new('RGB', (40, 60), color=(200, 200, 200)).save(path)
            paths.append(str(path))
        return paths

    def test_builds_without_error(self, tiny_images, tmp_path):
        import ipywidgets as widgets

        box = annotate_page_boundaries(
            tiny_images, tmp_path / 'boundaries.json', window_size=4, thumbnail_size=50,
        )
        assert isinstance(box, widgets.VBox)

    def test_raises_with_fewer_than_two_images(self, tmp_path):
        with pytest.raises(ValueError):
            annotate_page_boundaries(['only_one.jpg'], tmp_path / 'boundaries.json')

    def test_creates_an_empty_label_file_compatible_progress_state(self, tiny_images, tmp_path):
        label_file = tmp_path / 'boundaries.json'
        annotate_page_boundaries(tiny_images, label_file, window_size=4, thumbnail_size=50)
        # construction alone shouldn't write anything -- only actual marks (clicks/keypresses)
        # should trigger a save
        assert not label_file.exists()
