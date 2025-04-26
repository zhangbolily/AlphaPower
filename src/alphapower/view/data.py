from pydantic import BaseModel


class DataCategoryRefView(BaseModel):
    id: str
    name: str
