"""Integer max-flow (Dinic) and per-edge feasible min/max bounds.

The reconciliation feasibility problem is a bounded flow network::

        S -> supply nodes -> pair nodes -> demand nodes -> T

Each pair contributes two bounded edges (supply->pair and pair->demand)
with the same [lo, hi]. Lower bounds are turned into a circulation
feasibility problem with a super source/sink and a T->S back edge.

Per-pair minimum and maximum feasible values are found by pinning the
target pair to a candidate value and re-testing circulation feasibility.
The set of feasible pinned values for one edge is a contiguous integer
interval (the integral bounded-flow polytope projected onto a single
coordinate). We locate a feasible seed, then binary-search each boundary,
which is well-defined once a seed inside the interval is known.
All arithmetic is exact integer arithmetic (whole milliseconds).
"""
from __future__ import annotations

from collections import deque


class Edge:
    __slots__ = ("to", "rev", "cap")

    def __init__(self, to: int, rev: int, cap: int) -> None:
        self.to = to
        self.rev = rev
        self.cap = cap


class Dinic:
    def __init__(self, n: int) -> None:
        self.n = n
        self.g: list[list[Edge]] = [[] for _ in range(n)]
        self.level = [-1] * n
        self.it = [0] * n

    def add_edge(self, fr: int, to: int, cap: int) -> Edge:
        if cap < 0:
            raise ValueError("capacity must be non-negative")
        fwd = Edge(to, len(self.g[to]), cap)
        rev = Edge(fr, len(self.g[fr]), 0)
        self.g[fr].append(fwd)
        self.g[to].append(rev)
        return fwd

    def _bfs(self, s: int, t: int) -> bool:
        self.level = [-1] * self.n
        self.level[s] = 0
        q = deque([s])
        while q:
            v = q.popleft()
            for e in self.g[v]:
                if e.cap > 0 and self.level[e.to] < 0:
                    self.level[e.to] = self.level[v] + 1
                    q.append(e.to)
        return self.level[t] >= 0

    def _dfs(self, v: int, t: int, f: int) -> int:
        if v == t:
            return f
        gv = self.g[v]
        while self.it[v] < len(gv):
            e = gv[self.it[v]]
            if e.cap > 0 and self.level[e.to] == self.level[v] + 1:
                d = self._dfs(e.to, t, min(f, e.cap))
                if d > 0:
                    e.cap -= d
                    self.g[e.to][e.rev].cap += d
                    return d
            self.it[v] += 1
        return 0

    def max_flow(self, s: int, t: int) -> int:
        flow = 0
        big = 10 ** 30
        while self._bfs(s, t):
            self.it = [0] * self.n
            while True:
                f = self._dfs(s, t, big)
                if f == 0:
                    break
                flow += f
        return flow


class _Network:
    """Bounded-circulation feasibility for a fixed group topology."""

    def __init__(self, pair_keys, pair_lower, pair_upper, supply, demand,
                 pair_supply, pair_demand):
        self.pair_keys = pair_keys
        self.pair_lower = pair_lower
        self.pair_upper = pair_upper
        self.supply = supply
        self.demand = demand
        self.pair_supply = pair_supply
        self.pair_demand = pair_demand

        self.sup_idx = {sid: i for i, (sid, _) in enumerate(supply)}
        self.dem_idx = {did: i for i, (did, _) in enumerate(demand)}
        self.pair_idx = {k: i for i, k in enumerate(pair_keys)}
        self.n_sup = len(supply)
        self.n_dem = len(demand)
        self.n_pair = len(pair_keys)

        self.S = self.n_sup + self.n_pair + self.n_dem
        self.T = self.S + 1
        self.SS = self.T + 1
        self.TT = self.T + 2
        self.n_nodes = self.TT + 1

    def feasible(self, lower=None, upper=None) -> bool:
        lower = self.pair_lower if lower is None else lower
        upper = self.pair_upper if upper is None else upper
        d = Dinic(self.n_nodes)
        balance = [0] * self.n_nodes

        def add_bounded(fr, to, lo, hi):
            d.add_edge(fr, to, hi - lo)
            balance[fr] -= lo
            balance[to] += lo

        for sid, total in self.supply:
            add_bounded(self.S, self.sup_idx[sid], total, total)
        for did, total in self.demand:
            add_bounded(self.n_sup + self.n_pair + self.dem_idx[did],
                        self.T, total, total)
        for k in self.pair_keys:
            pi = self.n_sup + self.pair_idx[k]
            lo, hi = lower[k], upper[k]
            add_bounded(self.sup_idx[self.pair_supply[k]], pi, lo, hi)
            add_bounded(pi, self.n_sup + self.n_pair
                        + self.dem_idx[self.pair_demand[k]], lo, hi)

        required = 0
        for v, b in enumerate(balance):
            if b > 0:
                d.add_edge(self.SS, v, b)
                required += b
            elif b < 0:
                d.add_edge(v, self.TT, -b)
        d.add_edge(self.T, self.S, 10 ** 18)
        return d.max_flow(self.SS, self.TT) == required

    def feasible_flow(self):
        """Return a feasible allocation ``{pair_key: value}`` or None.

        The value on each pair is its lower bound plus the amount pushed on
        the supply->pair edge during the satisfying bounded circulation.
        """
        d = Dinic(self.n_nodes)
        balance = [0] * self.n_nodes
        edge_lo = {}

        def add_bounded(fr, to, lo, hi, remember=None):
            e = d.add_edge(fr, to, hi - lo)
            balance[fr] -= lo
            balance[to] += lo
            if remember is not None:
                edge_lo[remember] = (e, lo, hi)
            return e

        for sid, total in self.supply:
            add_bounded(self.S, self.sup_idx[sid], total, total)
        for did, total in self.demand:
            add_bounded(self.n_sup + self.n_pair + self.dem_idx[did],
                        self.T, total, total)
        for k in self.pair_keys:
            pi = self.n_sup + self.pair_idx[k]
            lo, hi = self.pair_lower[k], self.pair_upper[k]
            add_bounded(self.sup_idx[self.pair_supply[k]], pi, lo, hi,
                        remember=k)
            add_bounded(pi, self.n_sup + self.n_pair
                        + self.dem_idx[self.pair_demand[k]], lo, hi)

        required = 0
        for v, b in enumerate(balance):
            if b > 0:
                d.add_edge(self.SS, v, b)
                required += b
            elif b < 0:
                d.add_edge(v, self.TT, -b)
        d.add_edge(self.T, self.S, 10 ** 18)
        if d.max_flow(self.SS, self.TT) != required:
            return None

        flow = {}
        for k, (e, lo, hi) in edge_lo.items():
            pushed = (hi - lo) - e.cap
            flow[k] = lo + pushed
        return flow


def feasible_bounds(pair_keys, pair_lower, pair_upper, supply, demand,
                    pair_supply, pair_demand):
    """Exact feasible min/max flow for every pair in a group.

    Returns ``("feasible", {pair_key: (min_ms, max_ms)})`` or
    ``("infeasible", {})`` when no allocation satisfies every total.
    """
    net = _Network(pair_keys, pair_lower, pair_upper, supply, demand,
                   pair_supply, pair_demand)
    seed_flow = net.feasible_flow()
    if seed_flow is None:
        return "infeasible", {}

    result = {}
    for k in pair_keys:
        lo0, hi0 = pair_lower[k], pair_upper[k]
        seed = seed_flow[k]

        def feasible_at(v, key=k):
            lower = dict(pair_lower)
            upper = dict(pair_upper)
            lower[key] = upper[key] = v
            return net.feasible(lower, upper)

        # Feasibility is an integer interval and `seed` lies inside it, so on
        # each side of the seed the predicate is monotone toward the boundary.
        mn = _left_boundary(feasible_at, lo0, seed)
        mx = _right_boundary(feasible_at, seed, hi0)
        if mn is None or mx is None or mn > mx:
            return "infeasible", {}
        result[k] = (mn, mx)
    return "feasible", result


def _left_boundary(feasible_at, lo, seed):
    """Smallest feasible value; feasibility holds throughout [ans, seed]."""
    ans = seed
    left, right = lo, seed
    while left <= right:
        mid = (left + right) // 2
        if feasible_at(mid):
            ans = mid
            right = mid - 1
        else:
            left = mid + 1
    return ans


def _right_boundary(feasible_at, seed, hi):
    """Largest feasible value; feasibility holds throughout [seed, ans]."""
    ans = seed
    left, right = seed, hi
    while left <= right:
        mid = (left + right) // 2
        if feasible_at(mid):
            ans = mid
            left = mid + 1
        else:
            right = mid - 1
    return ans
