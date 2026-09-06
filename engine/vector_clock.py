from typing import Dict, Any, Optional
from enum import Enum

class CausalityRelation(Enum):
    EQUAL = "EQUAL"
    BEFORE = "BEFORE"         # V1 is ancestor of V2 (V2 is newer)
    AFTER = "AFTER"           # V1 is successor of V2 (V1 is newer)
    CONCURRENT = "CONCURRENT" # Conflict: divergent branches!


class VectorClock:
    """
    Implements Lamport / Fidge-Mattern Vector Clocks for causal ordering
    and conflict detection across cluster nodes.
    """
    def __init__(self, clock_dict: Optional[Dict[str, int]] = None):
        self._clock: Dict[str, int] = dict(clock_dict) if clock_dict else {}

    def get(self, node_id: str) -> int:
        return self._clock.get(node_id, 0)

    def increment(self, node_id: str) -> 'VectorClock':
        """Returns a new VectorClock with the given node's counter incremented."""
        new_clock = dict(self._clock)
        new_clock[node_id] = new_clock.get(node_id, 0) + 1
        return VectorClock(new_clock)

    def merge(self, other: 'VectorClock') -> 'VectorClock':
        """Computes the element-wise maximum of two vector clocks."""
        all_keys = set(self._clock.keys()) | set(other._clock.keys())
        merged = {k: max(self._clock.get(k, 0), other._clock.get(k, 0)) for k in all_keys}
        return VectorClock(merged)

    def compare(self, other: 'VectorClock') -> CausalityRelation:
        """
        Compares self with other:
        - EQUAL: self == other
        - BEFORE: self < other (other dominates self, other is strictly newer)
        - AFTER: self > other (self dominates other, self is strictly newer)
        - CONCURRENT: Neither dominates (conflict occurred due to concurrent writes)
        """
        all_keys = set(self._clock.keys()) | set(other._clock.keys())
        greater = False
        lesser = False

        for k in all_keys:
            v1 = self._clock.get(k, 0)
            v2 = other._clock.get(k, 0)
            if v1 > v2:
                greater = True
            elif v1 < v2:
                lesser = True

        if greater and lesser:
            return CausalityRelation.CONCURRENT
        elif greater:
            return CausalityRelation.AFTER
        elif lesser:
            return CausalityRelation.BEFORE
        else:
            return CausalityRelation.EQUAL

    def to_dict(self) -> Dict[str, int]:
        return dict(self._clock)

    @classmethod
    def from_dict(cls, data: Optional[Dict[str, int]]) -> 'VectorClock':
        return cls(data if data else {})

    def __repr__(self) -> str:
        return f"VectorClock({self._clock})"

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, VectorClock):
            return False
        return self.compare(other) == CausalityRelation.EQUAL
