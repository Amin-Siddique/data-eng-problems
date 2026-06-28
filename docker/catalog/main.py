"""Mock Unity Catalog API for local development."""

from datetime import datetime
from typing import Optional
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI(title="Lakehouse Local - Unity Catalog Mock")

# In-memory storage for mock catalog
catalogs: dict = {"my_catalog": {"schemas": {}}, "samples": {"schemas": {}}}
grants: list = []
lineage: list = []


class CatalogCreate(BaseModel):
    name: str
    comment: Optional[str] = None


class SchemaCreate(BaseModel):
    name: str
    catalog_name: str
    comment: Optional[str] = None


class Grant(BaseModel):
    principal: str
    privilege: str
    securable_type: str
    securable_name: str


@app.get("/")
async def root():
    return {"service": "Unity Catalog Mock", "status": "running"}


@app.get("/api/2.1/unity-catalog/catalogs")
async def list_catalogs():
    return {
        "catalogs": [
            {"name": name, "owner": "admin", "created_at": datetime.now().isoformat()}
            for name in catalogs.keys()
        ]
    }


@app.post("/api/2.1/unity-catalog/catalogs")
async def create_catalog(catalog: CatalogCreate):
    if catalog.name in catalogs:
        raise HTTPException(status_code=409, detail="Catalog already exists")
    catalogs[catalog.name] = {"schemas": {}, "comment": catalog.comment}
    return {"name": catalog.name, "owner": "admin", "created_at": datetime.now().isoformat()}


@app.get("/api/2.1/unity-catalog/schemas")
async def list_schemas(catalog_name: str):
    if catalog_name not in catalogs:
        raise HTTPException(status_code=404, detail="Catalog not found")
    return {
        "schemas": [
            {
                "name": name,
                "catalog_name": catalog_name,
                "owner": "admin",
            }
            for name in catalogs[catalog_name]["schemas"].keys()
        ]
    }


@app.post("/api/2.1/unity-catalog/schemas")
async def create_schema(schema: SchemaCreate):
    if schema.catalog_name not in catalogs:
        raise HTTPException(status_code=404, detail="Catalog not found")
    catalogs[schema.catalog_name]["schemas"][schema.name] = {"tables": {}}
    return {
        "name": schema.name,
        "catalog_name": schema.catalog_name,
        "owner": "admin",
    }


@app.get("/api/2.1/unity-catalog/tables")
async def list_tables(catalog_name: str, schema_name: str):
    if catalog_name not in catalogs:
        raise HTTPException(status_code=404, detail="Catalog not found")
    if schema_name not in catalogs[catalog_name]["schemas"]:
        raise HTTPException(status_code=404, detail="Schema not found")
    return {
        "tables": [
            {
                "name": name,
                "catalog_name": catalog_name,
                "schema_name": schema_name,
                "table_type": "MANAGED",
            }
            for name in catalogs[catalog_name]["schemas"][schema_name].get("tables", {}).keys()
        ]
    }


@app.post("/api/2.1/unity-catalog/permissions")
async def grant_permission(grant: Grant):
    grants.append(grant.model_dump())
    return {"status": "granted"}


@app.get("/api/2.1/unity-catalog/permissions/{securable_type}/{securable_name}")
async def get_permissions(securable_type: str, securable_name: str):
    matching = [
        g for g in grants
        if g["securable_type"] == securable_type and g["securable_name"] == securable_name
    ]
    return {"privilege_assignments": matching}


@app.get("/api/2.1/unity-catalog/lineage/table-lineage")
async def get_table_lineage(table_name: str):
    # Mock lineage - in reality would track actual queries
    return {
        "table_name": table_name,
        "upstream_tables": [],
        "downstream_tables": [],
    }


@app.get("/health")
async def health():
    return {"status": "healthy"}
