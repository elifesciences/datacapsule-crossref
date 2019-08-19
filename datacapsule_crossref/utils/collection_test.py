from .collection import (
    iter_sort_window
)

SOME_UNSORTABLE_OBJECT = object()


def _create_dict_for_index(i):
    return {'key': i, 'text': 'random text-{}'.format(i), 'other': SOME_UNSORTABLE_OBJECT}


def _create_dict_for_indices(indices):
    return [
        _create_dict_for_index(i)
        for i in indices
    ]


class TestIterSortWindow(object):
    def test_should_return_already_sorted_list(self):
        source = [1, 2, 3, 4, 5]
        result = list(iter_sort_window(source, 3))
        assert result == source

    def test_should_return_sorted_list_if_window_size_equals_total_size(self):
        source = [5, 4, 3, 2, 1]
        result = list(iter_sort_window(source, len(source)))
        assert result == sorted(source)

    def test_should_return_source_list_if_window_size_is_one(self):
        source = [5, 4, 3, 2, 1]
        result = list(iter_sort_window(source, 1))
        assert result == source

    def test_should_return_sorted_pairs_if_window_size_is_two(self):
        source = [5, 4, 3, 2, 1]
        result = list(iter_sort_window(source, 2))
        assert result == [4, 3, 2, 1, 5]

    def test_should_return_sorted_triples_if_window_size_is_three(self):
        source = [5, 4, 3, 2, 1]
        result = list(iter_sort_window(source, 3))
        assert result == [3, 2, 1, 4, 5]

    def test_should_return_sorted_window_with_key(self):
        source = [
            {'key': i, 'text': 'random text'}
            for i in [5, 4, 3, 2, 1]
        ]
        result = list(iter_sort_window(source, 3, key=lambda x: x['key']))
        assert result == [
            {'key': i, 'text': 'random text'}
            for i in [3, 2, 1, 4, 5]
        ]

    def test_should_remove_duplicates_id_enabled(self):
        source = [5, 3, 3, 3, 1]
        result = list(iter_sort_window(source, 3, remove_duplicates=True))
        print(result)
        assert result == [3, 1, 5]

    def test_should_remove_duplicates_id_enabled_with_key(self):
        source = _create_dict_for_indices([5, 3, 3, 3, 1])
        result = list(iter_sort_window(
            source, 3, key=lambda x: x['key'], remove_duplicates=True))
        print(result)
        assert result == _create_dict_for_indices([3, 1, 5])

    def test_should_remove_duplicates_id_enabled_with_key2(self):
        source = _create_dict_for_indices([5, 3, 3, 1, 1])
        result = list(iter_sort_window(
            source, 3, key=lambda x: x['key'], remove_duplicates=True))
        print(result)
        assert result == _create_dict_for_indices([3, 1, 5])
