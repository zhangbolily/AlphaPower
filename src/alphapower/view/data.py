from typing import Optional

from pydantic import BaseModel


class DataCategoryRefView(BaseModel):
    id: Optional[str]
    name: Optional[str]
