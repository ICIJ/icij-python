from functools import cached_property
from typing import Dict

from icij_common.pydantic_utils import ICIJModel


class PostgresConnectionInfo(ICIJModel):
    host: str = "127.0.0.1"
    password: str = "changeme"
    port: int = 5432
    use_ssl: bool = False
    user: str = "postgres"
    connect_timeout_s: float = 2.0

    def url(self, db: str = "postgres") -> str:
        url = f"postgres://{self.user}:{self.password}@{self.host}:{self.port}/{db}"
        if not self.use_ssl:
            url += "?sslmode=disable"
        return url

    @cached_property
    def kwargs(self) -> Dict:
        kwargs = self.dict()
        kwargs.pop("use_ssl")
        kwargs["connect_timeout"] = int(kwargs.pop("connect_timeout_s"))
        return kwargs
