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