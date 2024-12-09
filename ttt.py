import httpx
import os
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from dotenv import load_dotenv


@dataclass
class Column:
    name: str
    type: str
    codec: Optional[str]
    default_value: Optional[str]
    jsonpath: Optional[str]
    nullable: bool
    normalized_name: str


@dataclass
class Engine:
    engine: str
    engine_sorting_key: str
    engine_partition_key: str
    engine_primary_key: Optional[str]


@dataclass
class DataSource:
    id: str
    name: str
    engine: Engine
    columns: List[Column]
    indexes: List[Any]
    new_columns_detected: Dict[str, Any]
    quarantine_rows: int

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DataSource":
        engine = Engine(**data["engine"])
        columns = [Column(**col) for col in data["columns"]]
        return cls(
            id=data["id"],
            name=data["name"],
            engine=engine,
            columns=columns,
            indexes=data["indexes"],
            new_columns_detected=data["new_columns_detected"],
            quarantine_rows=data["quarantine_rows"],
        )


@dataclass
class Pipe:
    type: str
    id: str
    name: str
    description: Optional[str]
    endpoint: Optional[str]
    url: str

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Pipe":
        return cls(**data)


@dataclass
class PipeData:
    meta: List[Dict[str, str]]
    data: List[Dict[str, Any]]
    rows: int
    statistics: Dict[str, Any]

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PipeData":
        return cls(**data)


class APIClient:
    def __init__(self, api_url: str, token: str):
        self.api_url = api_url.rstrip("/")
        self.token = token
        self.client = httpx.AsyncClient(
            timeout=30.0,
            headers={"Accept": "application/json", "User-Agent": "Python/APIClient"},
        )

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def close(self):
        """Close the underlying HTTP client."""
        await self.client.aclose()

    async def _get(
        self, endpoint: str, params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        if params is None:
            params = {}
        params["token"] = self.token

        url = f"{self.api_url}/{endpoint}"
        response = await self.client.get(url, params=params)
        response.raise_for_status()
        return response.json()

    async def list_data_sources(self) -> List[DataSource]:
        """List all available data sources."""
        params = {"attrs": "id,name,description,columns"}
        response = await self._get("v0/datasources", params)
        return [DataSource.from_dict(ds) for ds in response["datasources"]]

    async def get_data_source(self, datasource_id: str) -> Dict[str, Any]:
        """Get detailed information about a specific data source."""
        params = {"debug": "columns", "include_workspace_names": "true"}
        return await self._get(f"v0/datasources/{datasource_id}", params)

    async def list_pipes(self) -> List[Pipe]:
        """List all available pipes."""
        params = {"attrs": "id,name,description,type,endpoint"}
        response = await self._get("v0/pipes", params)
        return [Pipe.from_dict(pipe) for pipe in response["pipes"]]

    async def get_pipe(self, pipe_name: str) -> Dict[str, Any]:
        """Get detailed information about a specific pipe."""
        return await self._get(f"v0/pipes/{pipe_name}")

    async def get_pipe_data(self, pipe_name: str, **params) -> PipeData:
        """Get data from a pipe with optional parameters."""
        response = await self._get(f"v0/pipes/{pipe_name}.json", params)
        return PipeData.from_dict(response)

    async def run_select_query(self, query: str) -> Dict[str, Any]:
        """Run a SQL SELECT query."""
        params = {"q": f"{query} FORMAT JSON"}
        return await self._get("v0/sql", params)


# Example usage:
import asyncio


async def main():
    load_dotenv()
    TB_API_URL = os.getenv("TB_API_URL")
    TB_ADMIN_TOKEN = os.getenv("TB_ADMIN_TOKEN")
    async with APIClient(api_url=TB_API_URL, token=TB_ADMIN_TOKEN) as client:
        # List data sources
        data_sources = await client.list_data_sources()
        for ds in data_sources:
            print(f"Data source: {ds.name} ({ds.id})")

        # Get specific data source
        ds_info = await client.get_data_source("t_745a260d6ae94f5088a3fd5b34d31e2a")
        print(f"Data source details: {ds_info}")

        # List pipes
        pipes = await client.list_pipes()
        for pipe in pipes:
            print(f"Pipe: {pipe.name} ({pipe.type})")

        # Get specific pipe
        pipe_info = await client.get_pipe("endpoint_to_paginate")
        print(f"Pipe details: {pipe_info}")

        # Get pipe data with parameters
        pipe_data = await client.get_pipe_data("endpoint_to_paginate", year=2024)
        print(f"Pipe data: {pipe_data}")

        # Run SQL query
        query_result = await client.run_select_query(
            "SELECT * FROM playground_1796_2 LIMIT 10"
        )
        print(f"Query result: {query_result}")


if __name__ == "__main__":
    asyncio.run(main())
