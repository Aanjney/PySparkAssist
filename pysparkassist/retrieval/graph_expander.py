from pysparkassist.ingest.entities import EntityGraph


def expand_entities(entity_names: list[str], graph: EntityGraph, max_depth: int = 1) -> set[str]:
    """Expand entity names by traversing the knowledge graph."""
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
        frontier = next_frontier

    return expanded
