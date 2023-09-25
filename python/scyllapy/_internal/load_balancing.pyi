class LatencyAwareness:
    def __init__(
        self,
        *,
        minimum_measurements: int | None = None,
        retry_period: int | None = None,
        exclusion_threshold: float | None = None,
        update_rate: int | None = None,
        scale: int | None = None,
    ) -> None: ...
    """
    Build latency awareness for balancing policy.

    :param minimum_measurements: Minimum number of measurements to consider a host
        as eligible for query plans.
    :param retry_period: Number of milliseconds to wait before attempting to refresh
        the latency information of a host.
    :param exclusion_threshold: Maximum ratio of hosts to exclude from query plans.
        For example, if set to 2, the resulting policy excludes nodes that are
        more than twice slower than the fastest node.
    :param update_rate: Number of milliseconds between measurements.
    :param scale: provides control on how the weight given to older latencies decreases
        over time.
    """

class LoadBalancingPolicy:
    """
    Load balancing policy.

    Useful to control how the driver distributes queries among nodes.
    Can be applied to profiles.
    """

    @classmethod
    async def build(
        cls,
        *,
        token_aware: bool | None = None,
        prefer_rack: str | None = None,
        prefer_datacenter: str | None = None,
        permit_dc_failover: bool | None = None,
        shuffling_replicas: bool | None = None,
    ) -> LoadBalancingPolicy: ...
    """
    Construct load balancing policy.

    It requires to be async, becausse it needs to start a background task.

    :param token_aware: Whether to use token aware routing.
    :param prefer_rack: Name of the rack to prefer.
    :param prefer_datacenter: Name of the datacenter to prefer.
    :param permit_dc_failover: Whether to allow datacenter failover.
    :param shuffling_replicas: Whether to shuffle replicas.
    """

    async def with_latency_awareness(
        self,
        latency_awareness: LatencyAwareness,
    ) -> None: ...
