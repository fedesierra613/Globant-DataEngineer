from pydantic import BaseModel, conlist
from datetime import datetime
from typing import List


class DepartmentBase(BaseModel):
    id: int
    department: str

class DepartmentCreate(DepartmentBase):
    pass


class JobBase(BaseModel):
    id: int
    job: str

class JobCreate(JobBase):
    pass


class HiredEmployeeBase(BaseModel):
    id: int
    name: str
    datetime: datetime
    department_id: int
    job_id: int

class HiredEmployeeCreate(HiredEmployeeBase):
    pass


class Department(DepartmentBase):
    class Config:
        orm_mode = True

class Job(JobBase):
    class Config:
        orm_mode = True

class HiredEmployee(HiredEmployeeBase):
    class Config:
        orm_mode = True

class QuarterlyHires(BaseModel):
    department: str
    job: str
    Q1: int
    Q2: int
    Q3: int
    Q4: int

    class Config:
        orm_mode = True 


class DepartmentHires(BaseModel):
    id: int
    department: str
    hired: int

    class Config:
        orm_mode = True 