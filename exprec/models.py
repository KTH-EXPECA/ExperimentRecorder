#  Copyright (c) 2021 KTH Royal Institute of Technology
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
from __future__ import annotations

import datetime
import uuid
from typing import Optional

from sqlalchemy import CHAR, Column, DateTime, ForeignKey, Numeric, \
    Text, \
    TypeDecorator, UniqueConstraint, event
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.engine import Engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()


@event.listens_for(Engine, "connect")
def _set_sqlite_pragma(dbapi_connection, connection_record):
    """
    Enforces per-connection settings. In particular, we use it to enable
    FOREIGN KEY constraint checking.

    https://stackoverflow.com/a/31797403/5035798
    """

    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()


class GUID(TypeDecorator):
    """Platform-independent GUID type.

    Uses PostgreSQL's UUID type, otherwise uses
    CHAR(32), storing as stringified hex values.


    Taken from: https://docs.sqlalchemy.org/en/13/core/custom_types.html
    """
    impl = CHAR

    def load_dialect_impl(self, dialect):
        if dialect.name == 'postgresql':
            return dialect.type_descriptor(UUID())
        else:
            return dialect.type_descriptor(CHAR(32))

    @staticmethod
    def _process_param(value, dialect) -> Optional[str]:
        if value is None:
            return value
        elif dialect.name == 'postgresql':
            return str(value)
        else:
            if not isinstance(value, uuid.UUID):
                return f'{uuid.UUID(value).int:032x}'
            else:
                # hexstring
                return f'{value.int:032x}'

    def process_bind_param(self, value, dialect):
        return self._process_param(value, dialect)

    def process_literal_param(self, value, dialect):
        return self._process_param(value, dialect)

    def process_result_value(self, value, dialect):
        if value is None:
            return value
        else:
            if not isinstance(value, uuid.UUID):
                value = uuid.UUID(value)
            return value

    @property
    def python_type(self) -> type:
        return uuid.UUID


class ExperimentInstance(Base):
    __tablename__ = 'instances'

    id = Column(GUID,
                primary_key=True,
                default=uuid.uuid4,
                nullable=False)
    timestamp = Column(DateTime,
                       nullable=False,
                       default=datetime.datetime.now)

    experiment_metadata = relationship('ExperimentMetadata',
                                       order_by='ExperimentMetadata.label',
                                       back_populates='instance')
    variables = relationship('InstanceVariable',
                             order_by='InstanceVariable.name',
                             back_populates='instance')


class ExperimentMetadata(Base):
    __tablename__ = 'instance_metadata'

    instance_id = Column(GUID,
                         ForeignKey('instances.id',
                                    ondelete='SET NULL',
                                    onupdate='CASCADE'),
                         nullable=False,
                         primary_key=True)
    label = Column(Text,
                   nullable=False,
                   primary_key=True)
    value = Column(Text, nullable=True)

    instance = relationship('ExperimentInstance',
                            back_populates='experiment_metadata')


class InstanceVariable(Base):
    __tablename__ = 'variables'

    id = Column(GUID,
                primary_key=True,
                nullable=False,
                default=uuid.uuid4)

    name = Column(Text, nullable=False)
    instance_id = Column(GUID,
                         ForeignKey('instances.id',
                                    onupdate='CASCADE',
                                    ondelete='SET NULL'),
                         nullable=False)
    timestamp = Column(DateTime,
                       default=datetime.datetime.now,
                       nullable=False)

    instance = relationship('ExperimentInstance', back_populates='variables')
    records = relationship('VariableRecord',
                           order_by='VariableRecord.timestamp',
                           back_populates='variable')

    name_instance_constraint = UniqueConstraint(name, instance_id)


class VariableRecord(Base):
    __tablename__ = 'records'

    variable_id = Column(GUID,
                         ForeignKey('variables.id',
                                    onupdate='CASCADE',
                                    ondelete='SET NULL'),
                         nullable=False,
                         primary_key=True)
    timestamp = Column(DateTime,
                       nullable=False,
                       primary_key=True)
    value = Column(Numeric,
                   nullable=True)

    variable = relationship('InstanceVariable', back_populates='records')
