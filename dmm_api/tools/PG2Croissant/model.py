from dataclasses import dataclass
from typing import List, Optional, Dict   

@dataclass 
class FileObject:
    id: str
    properties: Dict[str, str]

@dataclass
class ColumnStatistics:
    id: str
    properties: Dict[str, str]

@dataclass
class Field:
    id: str
    properties: Dict[str, str]
    statistics: List[ColumnStatistics]

@dataclass
class RecordSet:
    id: str
    fields: list[Field]
    properties: Dict[str, str]

@dataclass
class Dataset:
    id: str
    distribution: List[FileObject]
    recordSets: List[RecordSet]
    properties: Dict[str, str]

