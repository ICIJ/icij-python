# syntax=docker/dockerfile:1.14.0
FROM python:3.11-slim-bullseye AS python-base

ENV HOME=/home/user
WORKDIR $HOME
RUN apt-get update && apt-get install -y curl

RUN curl -LsSf https://astral.sh/uv/0.6.7/install.sh | sh
ENV PATH="$HOME/.local/bin:$PATH"
ENV UV_LINK_MODE=copy
ENV UV_COMPILE_BYTECODE=1

##### HTTP serivce
FROM python-base AS http-service

ARG dbmate_arch
WORKDIR $HOME/src/app
RUN curl -fsSL -o /usr/local/bin/dbmate https://github.com/amacneil/dbmate/releases/download/v2.19.0/dbmate-linux-${dbmate_arch} \
    && chmod +x /usr/local/bin/dbmate
# Install deps first to optimize layer cache
RUN --mount=type=cache,target=~/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync -v --frozen --no-editable --no-sources --no-install-project --extra http --extra amqp --extra postgres
# Then copy code
ADD uv.lock pyproject.toml README.md ./
ADD icij_worker ./icij_worker/
# Then install service
RUN uv sync -v --frozen --no-editable --no-sources --extra http --extra amqp --extra postgres
RUN uv pip list
RUN rm -rf ~/.cache/pip $(uv cache dir)

ENTRYPOINT ["uv", "run", "--frozen", "--no-editable", "--no-sources", "icij-http-server"]
