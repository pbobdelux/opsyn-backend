from datetime import datetime, timezone
from sqlalchemy import Column, String, DateTime, Boolean, ForeignKey
from sqlalchemy.orm import relationship
from database import Base


class Organization(Base):
    __tablename__ = "organizations"

    id = Column(String, primary_key=True)
    org_code = Column(String, unique=True, nullable=True)
    slug = Column(String, unique=True, nullable=False)
    name = Column(String, nullable=False)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))


class Brand(Base):
    __tablename__ = "brands"

    id = Column(String, primary_key=True)
    org_id = Column(String, ForeignKey("organizations.id"), nullable=False)
    slug = Column(String, nullable=False)
    name = Column(String, nullable=False)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))


class Employee(Base):
    __tablename__ = "employees"

    id = Column(String, primary_key=True)
    org_id = Column(String, ForeignKey("organizations.id"), nullable=False)
    first_name = Column(String, nullable=False)
    last_name = Column(String, nullable=False)
    email = Column(String, unique=True, nullable=False)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(
        DateTime,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    passcodes = relationship("EmployeePasscode", back_populates="employee")
    brand_access = relationship("EmployeeBrandAccess", back_populates="employee")
    app_access = relationship("EmployeeAppAccess", back_populates="employee")


class EmployeePasscode(Base):
    __tablename__ = "employee_passcodes"

    id = Column(String, primary_key=True)
    employee_id = Column(String, ForeignKey("employees.id"), nullable=False)
    passcode_hash = Column(String, nullable=False)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(
        DateTime,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    employee = relationship("Employee", back_populates="passcodes")


class EmployeeBrandAccess(Base):
    __tablename__ = "employee_brand_access"

    id = Column(String, primary_key=True)
    employee_id = Column(String, ForeignKey("employees.id"), nullable=False)
    brand_id = Column(String, ForeignKey("brands.id"), nullable=False)
    role = Column(String, default="viewer")  # admin, editor, viewer
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    employee = relationship("Employee", back_populates="brand_access")


class EmployeeAppAccess(Base):
    __tablename__ = "employee_app_access"

    id = Column(String, primary_key=True)
    employee_id = Column(String, ForeignKey("employees.id"), nullable=False)
    app_id = Column(String, nullable=False)  # brand_app, driver_app, crm_app
    role = Column(String, default="viewer")  # admin, editor, viewer
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    employee = relationship("Employee", back_populates="app_access")
