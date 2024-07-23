from icij_worker.typing_ import PercentProgress, RawProgress


def to_raw_progress(progress: PercentProgress, max_progress: int) -> RawProgress:
    if not max_progress > 0:
        raise ValueError("max_progress must be > 0")

    async def raw(p: int):
        await progress(p / max_progress * 100)

    return raw


def to_scaled_progress(
    progress: PercentProgress, *, start: float = 0.0, end: float = 1.0
):
    if not 0 <= start < end:
        raise ValueError("start must be [0, end[")
    if not start < end <= 100:
        raise ValueError("end must be ]start, 1.0]")

    async def _scaled(p: float):
        await progress(start + p * (end - start))

    return _scaled
