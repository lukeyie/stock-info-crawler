from typing import List, Optional
from pydantic import BaseModel


class DateInfoDTO(BaseModel):
    date: str
    open: str
    close: str
    high: str
    low: str
    volume: str


class PriceVolumeDTO(BaseModel):
    ticker: str
    stock_name: str
    date_info: Optional[List[DateInfoDTO]]
