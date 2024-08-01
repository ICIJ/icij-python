from icij_worker.typing_ import RateProgress, RawProgress


def to_raw_progress(progress: RateProgress, max_progress: int) -> RawProgress:
    if not max_progress > 0:
        raise ValueError("max_progress must be > 0")

    async def raw(p: int):
        await progress(p / max_progress)

    return raw


def to_scaled_progress(progress: RateProgress, *, start: float = 0.0, end: float = 1.0):
    if not 0 <= start < end:
        raise ValueError("start must be [0, end[")
    if not start < end <= 1.0:
        raise ValueError("end must be ]start, 1.0]")

    async def _scaled(p: float):
        await progress(start + p * (end - start))

    return _scaled
