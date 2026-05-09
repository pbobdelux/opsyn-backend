import uuid
from datetime import datetime, timezone
from sqlalchemy import Column, String, DateTime, Boolean, ForeignKey
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import relationship
from database import Base


def _utc_now():
    return datetime.now(timezone.utc)


class Organization(Base):
    __tablename__ = "organizations"

    id = Column(PG_UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_code = Column(String, unique=True, nullable=True)
    slug = Column(String, unique=True, nullable=False)
    name = Column(String, nullable=False)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=_utc_now)


class Brand(Base):
    __tablename__ = "brands"

    id = Column(PG_UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(PG_UUID(as_uuid=True), ForeignKey("organizations.id"), nullable=False)
    slug = Column(String, nullable=False)
    name = Column(String, nullable=False)
    # is_active removed - column doesn't exist in database
    created_at = Column(DateTime, default=_utc_now)


class Employee(Base):
    __tablename__ = "employees"

    id = Column(PG_UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(PG_UUID(as_uuid=True), ForeignKey("organizations.id"), nullable=False)
    first_name = Column(String, nullable=False)
    last_name = Column(String, nullable=False)
    email = Column(String, unique=True, nullable=False)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=_utc_now)
    updated_at = Column(
        DateTime,
        default=_utc_now,
        onupdate=_utc_now,
    )

    passcodes = relationship("EmployeePasscode", back_populates="employee")
    brand_access = relationship("EmployeeBrandAccess", back_populates="employee")
    app_access = relationship("EmployeeAppAccess", back_populates="employee")


class EmployeePasscode(Base):
    __tablename__ = "employee_passcodes"

    id = Column(PG_UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    employee_id = Column(PG_UUID(as_uuid=True), ForeignKey("employees.id"), nullable=False)
    passcode_hash = Column(String, nullable=False)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=_utc_now)
    updated_at = Column(
        DateTime,
        default=_utc_now,
        onupdate=_utc_now,
    )

    employee = relationship("Employee", back_populates="passcodes")


class EmployeeBrandAccess(Base):
    __tablename__ = "employee_brand_access"

    id = Column(PG_UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    employee_id = Column(PG_UUID(as_uuid=True), ForeignKey("employees.id"), nullable=False)
    brand_id = Column(PG_UUID(as_uuid=True), ForeignKey("brands.id"), nullable=False)
    role = Column(String, default="viewer")  # admin, editor, viewer
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=_utc_now)

    employee = relationship("Employee", back_populates="brand_access")


class EmployeeAppAccess(Base):
    __tablename__ = "employee_app_access"

    id = Column(PG_UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    employee_id = Column(PG_UUID(as_uuid=True), ForeignKey("employees.id"), nullable=False)
    app_id = Column(String, nullable=False)  # brand_app, driver_app, crm_app
    role = Column(String, default="viewer")  # admin, editor, viewer
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=_utc_now)

    employee = relationship("Employee", back_populates="app_access")
