from pysparkassist.ingest.entities import EntityGraph


def expand_entities(
    entity_names: list[str],
    graph: EntityGraph,
    max_depth: int = 1,
    max_expansion: int = 15,
) -> set[str]:
    """Expand entity names by traversing the knowledge graph.

    Caps total expansion to max_expansion to prevent highly connected
    hub nodes from returning overly broad filter sets.
    """
    if not entity_names:
        return set()

    expanded = set(entity_names)
    frontier = list(entity_names)

    for _ in range(max_depth):
        next_frontier: list[str] = []
        for name in frontier:
            related = graph.get_related_entities(name)
            for entity in related:
                if entity.name not in expanded:
                    expanded.add(entity.name)
                    next_frontier.append(entity.name)
                    if len(expanded) >= max_expansion:
                        return expanded
        frontier = next_frontier

    return expanded
