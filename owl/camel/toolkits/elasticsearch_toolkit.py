import os
from typing import Any, Dict, List, Optional

from elasticsearch import Elasticsearch
from camel.toolkits.base import BaseToolkit
from loguru import logger
from camel.toolkits.base import FunctionTool

class ElasticsearchToolkit(BaseToolkit):
    r"""A class representing a toolkit for interacting with Elasticsearch.

    This class provides methods for searching and indexing documents in
    an Elasticsearch cluster.
    """

    def __init__(self, hosts: Optional[List[str]] = None, username: Optional[str] = None, password: Optional[str] = None):
        self.hosts = hosts or [os.getenv("ELASTICSEARCH_HOST", "https://localhost:9200")]
        self.username = username or os.getenv("ELASTICSEARCH_USERNAME")
        self.password = password or os.getenv("ELASTICSEARCH_PASSWORD")
        self.client = Elasticsearch(self.hosts, http_auth=(self.username, self.password))

    def search(self, index: str, query: Dict[str, Any]) -> Dict[str, Any]:
        r"""Search for documents in the specified Elasticsearch index.

        Args:
            index (str): The name of the Elasticsearch index.
            query (Dict[str, Any]): The search query.

        Returns:
            Dict[str, Any]: The search results.
        """
        logger.debug(f"Searching in index '{index}' with query: {query}")
        try:
            response = self.client.search(index=index, body=query)
            return response
        except Exception as e:
            logger.error(f"An error occurred during the search: {e}")
            return {"error": str(e)}

    def index_document(self, index: str, document: Dict[str, Any], doc_id: Optional[str] = None) -> Dict[str, Any]:
        r"""Index a document in the specified Elasticsearch index.

        Args:
            index (str): The name of the Elasticsearch index.
            document (Dict[str, Any]): The document to be indexed.
            doc_id (Optional[str]): The ID of the document. If not provided, Elasticsearch will generate one.

        Returns:
            Dict[str, Any]: The indexing result.
        """
        logger.debug(f"Indexing document in index '{index}' with ID '{doc_id}': {document}")
        try:
            response = self.client.index(index=index, id=doc_id, body=document)
            return response
        except Exception as e:
            logger.error(f"An error occurred during the indexing: {e}")
            return {"error": str(e)}

    def cluster_health(self) -> Dict[str, Any]:
        r"""Get the health status of the Elasticsearch cluster.

        Returns:
            Dict[str, Any]: The health status of the cluster.
        """
        logger.debug("Getting cluster health status")
        try:
            response = self.client.cluster.health()
            return response
        except Exception as e:
            logger.error(f"An error occurred while getting cluster health: {e}")
            return {"error": str(e)}

    def cluster_stats(self) -> Dict[str, Any]:
        r"""Get the statistics of the Elasticsearch cluster.

        Returns:
            Dict[str, Any]: The statistics of the cluster.
        """
        logger.debug("Getting cluster statistics")
        try:
            response = self.client.cluster.stats()
            return response
        except Exception as e:
            logger.error(f"An error occurred while getting cluster statistics: {e}")
            return {"error": str(e)}

    def get_tools(self) -> List[FunctionTool]:
        r"""Returns a list of FunctionTool objects representing the
        functions in the toolkit.

        Returns:
            List[FunctionTool]: A list of FunctionTool objects
                representing the functions in the toolkit.
        """
        return [
            FunctionTool(self.search),
            FunctionTool(self.index_document),
            FunctionTool(self.cluster_health),
            FunctionTool(self.cluster_stats),
        ]
