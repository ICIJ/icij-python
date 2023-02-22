from typing import Dict, Optional

import neo4j
import pytest
import pytest_asyncio

from neo4j_app.core.documents import import_documents
from neo4j_app.core.elasticsearch import ESClient
from neo4j_app.core.objects import IncrementalImportResponse
from neo4j_app.tests.conftest import NEO4J_TEST_IMPORT_DIR, index_docs, index_noise


@pytest_asyncio.fixture(scope="module")
async def _populate_es(es_test_client_module: ESClient):
    es_client = es_test_client_module
    index_name = es_client.project_index
    # Index some Documents
    n_docs = 10
    async for _ in index_docs(es_client, index_name=index_name, n=n_docs):
        pass
    # Index other entities which we don't want to import
    n_noise = 10
    async for _ in index_noise(es_client, index_name=index_name, n=n_noise):
        pass
    yield es_client


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "query,doc_type_field,expected_response",
    [
        # No query, let's check that only documents are inserted and not noise
        (None, "type", IncrementalImportResponse(n_to_insert=10, n_inserted=10)),
        # Match all query, let's check that only documents are inserted and not noise
        (
            {"match_all": {}},
            "type",
            IncrementalImportResponse(n_to_insert=10, n_inserted=10),
        ),
        # Term query, let's check that only the right doc is inserted
        (
            {"ids": {"values": ["doc-0"]}},
            "type",
            IncrementalImportResponse(n_to_insert=1, n_inserted=1),
        ),
        # Let's check that the doc_type_field is taken into account
        (
            None,
            "fieldThatDoesNotExists",
            IncrementalImportResponse(n_to_insert=0, n_inserted=0),
        ),
    ],
)
async def test_import_documents(
    _populate_es: ESClient,
    query: Optional[Dict],
    doc_type_field: str,
    expected_response: IncrementalImportResponse,
    neo4j_test_session: neo4j.AsyncSession,
):
    # pylint: disable=invalid-name
    # Given
    es_client = _populate_es
    scroll = "1m"
    scroll_size = 3  # Let's use a odd number

    # When
    response = await import_documents(
        neo4j_session=neo4j_test_session,
        es_client=es_client,
        neo4j_import_dir=NEO4J_TEST_IMPORT_DIR,
        query=query,
        scroll=scroll,
        scroll_size=scroll_size,
        doc_type_field=doc_type_field,
    )

    # Then
    assert response == expected_response
