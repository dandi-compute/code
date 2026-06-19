import collections
import random

from ..dandiset._load_content_id_to_usage_dandiset_path import _load_content_id_to_usage_dandiset_path


def _order_content_ids_for_uniform_dandiset_sampling(*, content_ids: list[str]) -> list[str]:
    """
    Return content IDs ordered by randomized round-robin across source Dandisets.

    Content IDs without exactly one mapped source Dandiset are kept as their own
    singleton groups so they remain eligible without being merged into the
    randomization for any unrelated Dandiset.
    """
    content_id_to_dandiset_path = _load_content_id_to_usage_dandiset_path()
    content_ids_by_dandiset: dict[str | tuple[str, str], list[str]] = {}
    for content_id in content_ids:
        content_id_mapping = content_id_to_dandiset_path.get(content_id, {})
        if len(content_id_mapping) == 1:
            dandiset_key: str | tuple[str, str] = next(iter(content_id_mapping))
        else:
            dandiset_key = ("content-id", content_id)
        content_ids_by_dandiset.setdefault(dandiset_key, []).append(content_id)

    grouped_content_ids = list(content_ids_by_dandiset.values())
    random.shuffle(grouped_content_ids)
    grouped_content_id_queues: list[collections.deque[str]] = []
    for content_ids_for_dandiset in grouped_content_ids:
        random.shuffle(content_ids_for_dandiset)
        grouped_content_id_queues.append(collections.deque(content_ids_for_dandiset))

    ordered_content_ids: list[str] = []
    while grouped_content_id_queues:
        next_grouped_content_id_queues: list[collections.deque[str]] = []
        for content_ids_for_dandiset in grouped_content_id_queues:
            ordered_content_ids.append(content_ids_for_dandiset.popleft())
            if content_ids_for_dandiset:
                next_grouped_content_id_queues.append(content_ids_for_dandiset)
        grouped_content_id_queues = next_grouped_content_id_queues
    return ordered_content_ids
