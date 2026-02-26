from dataclasses import dataclass
from typing import List, Optional   

@dataclass
class Column:
    name: str
    type: str

@dataclass 
class Table:
    name: str
    columns: List[Column]