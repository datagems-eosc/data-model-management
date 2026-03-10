from dataclasses import dataclass
from typing import List, Dict   

@dataclass 
class FileObject:
    id: str
    properties: Dict[str, str]

@dataclass
class ColumnStatistics:
    id: str
    properties: Dict[str, str]

@dataclass
class Source:
    extract: Dict[str, str]
    fileObject: str

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
    recordSet: List[RecordSet]
    properties: Dict[str, str]

