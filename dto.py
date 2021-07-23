import decimal
from typing import List, Optional
from numpy import double
from pydantic import BaseModel, Field, root_validator


class DateInfoDTO(BaseModel):
    date: Optional[str]
    open: Optional[double]
    close: Optional[double]
    high: Optional[double]
    low: Optional[double]
    volume: Optional[int]

    class Config:
        validate_assignment = True


class IncomeStatementDTO(BaseModel):
    year: Optional[int]
    season: Optional[int]
    # Net Operating Revenue
    nor: Optional[int]
    # Cost of Goods Sold or Manufacturing
    cost: Optional[int]
    # Gross Profit
    pro: Optional[int]
    # Operating Expenses
    oe: Optional[int]
    # Operating Income
    oi: Optional[int]
    # Total Non-operating Revenue
    nor: Optional[int]
    # Income before Tax
    btax: Optional[int]
    # Net Income
    ni: Optional[int]
    # Earnings Per Share
    eps: Optional[double]

    class Config:
        validate_assignment = True


class StockDTO(BaseModel):
    ticker: str
    stock_name: str
    date_info: List[DateInfoDTO] = Field(default_factory=list)
    income_statements: List[IncomeStatementDTO] = Field(default_factory=list)

    class Config:
        validate_assignment = True


class StockInfoDTO(BaseModel):
    stock_name: Optional[str]
    sector: Optional[str]
    market: Optional[str]
