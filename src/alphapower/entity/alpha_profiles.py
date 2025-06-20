from datetime import datetime
from typing import Any, List

from sqlalchemy import JSON, DateTime, Enum, Index, Integer, String, Text, func
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import MappedColumn, mapped_column

from alphapower.constants import ALPHA_ID_LENGTH, AlphaType
from alphapower.view.alpha import StringListAdapter

from .alphas import Base


class AlphaProfile(Base):
    __tablename__ = "alpha_profiles"

    id: MappedColumn[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    alpha_id: MappedColumn[str] = mapped_column(
        String(ALPHA_ID_LENGTH),
        nullable=False,
        unique=True,
        index=True,
        comment="Alpha ID",  # 添加字段注释
    )
    type: MappedColumn[AlphaType] = mapped_column(
        Enum(AlphaType), nullable=False, default=AlphaType.DEFAULT
    )
    _used_data_fields: MappedColumn[JSON] = mapped_column(
        JSON, nullable=False, default=dict, name="used_data_fields"
    )
    _used_operators: MappedColumn[JSON] = mapped_column(
        JSON, nullable=False, default=dict, name="used_operators"
    )
    regular_hash: MappedColumn[str] = mapped_column(
        String(64),
        nullable=True,
        unique=False,
        comment="Regular 表达式指纹",  # 添加字段注释
    )
    normalized_regular: MappedColumn[str] = mapped_column(
        Text,
        nullable=True,
        comment="Normalized Regular 表达式",  # 添加字段注释
    )
    regular_fingerprint: MappedColumn[str] = mapped_column(
        String(64),
        nullable=True,
        unique=False,
        index=True,
        comment="Normalized Regular 表达式指纹",  # 添加字段注释
    )
    selection_hash: MappedColumn[str] = mapped_column(
        String(64),
        nullable=True,
        unique=False,
        index=True,
        comment="Selection 表达式指纹",  # 添加字段注释
    )
    normalized_selection: MappedColumn[str] = mapped_column(
        Text,
        nullable=True,
        comment="Normalized Selection 表达式",  # 添加字段注释
    )
    selection_fingerprint: MappedColumn[str] = mapped_column(
        String(64),
        nullable=True,
        unique=False,
        index=True,
        comment="Normalized Selection 表达式指纹",  # 添加字段注释
    )
    combo_hash: MappedColumn[str] = mapped_column(
        String(64),
        nullable=True,
        unique=False,
        index=True,
        comment="Combo 表达式指纹",  # 添加字段注释
    )
    normalized_combo: MappedColumn[str] = mapped_column(
        Text,
        nullable=True,
        comment="Normalized Combo 表达式",  # 添加字段注释
    )
    combo_fingerprint: MappedColumn[str] = mapped_column(
        String(64),
        nullable=True,
        unique=False,
        index=True,
        comment="Normalized Combo 表达式指纹",  # 添加字段注释
    )
    updated_at: MappedColumn[datetime] = mapped_column(
        DateTime,
        nullable=False,
        insert_default=func.now(),  # pylint: disable=E1102
        onupdate=func.now(),  # pylint: disable=E1102
        comment="最后更新时间",  # 添加字段注释
    )
    created_at: MappedColumn[datetime] = mapped_column(
        DateTime,
        nullable=False,
        insert_default=func.now(),  # pylint: disable=E1102
        comment="创建时间",  # 添加字段注释
    )

    def __init__(self, **kwargs: Any) -> None:
        used_data_fields: List[str] = kwargs.pop("used_data_fields", [])
        used_operators: List[str] = kwargs.pop("used_operators", [])

        super().__init__(**kwargs)

        if isinstance(used_data_fields, list):
            self.used_data_fields = used_data_fields  # type: ignore[method-assign]

        if isinstance(used_operators, list):
            self.used_operators = used_operators  # type: ignore[method-assign]

    @hybrid_property
    def used_data_fields(self) -> List[str]:
        if self._used_data_fields is None:
            return []

        used_data_fields: List[str] = StringListAdapter.validate_python(
            self._used_data_fields
        )
        return used_data_fields

    @used_data_fields.setter  # type: ignore[no-redef]
    def used_data_fields(self, value: List[str]) -> None:
        if value is None:
            self._used_data_fields = None
        else:
            self._used_data_fields = StringListAdapter.dump_python(
                value,
                mode="json",
            )

    @hybrid_property
    def used_operators(self) -> List[str]:
        if self._used_operators is None:
            return []

        used_operators: List[str] = StringListAdapter.validate_python(
            self._used_operators
        )
        return used_operators

    @used_operators.setter  # type: ignore[no-redef]
    def used_operators(self, value: List[str]) -> None:
        if value is None:
            self._used_operators = None
        else:
            self._used_operators = StringListAdapter.dump_python(
                value,
                mode="json",
            )


class AlphaProfileDataFields(Base):
    __tablename__ = "alpha_profile_data_fields"

    id: MappedColumn[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    alpha_id: MappedColumn[str] = mapped_column(
        String(ALPHA_ID_LENGTH),
        nullable=False,
        index=True,
        comment="Alpha ID",  # 添加字段注释
    )
    data_field: MappedColumn[str] = mapped_column(
        String(256),
        nullable=False,
        index=True,
    )
    updated_at: MappedColumn[datetime] = mapped_column(
        DateTime,
        nullable=False,
        insert_default=func.now(),  # pylint: disable=E1102
        onupdate=func.now(),  # pylint: disable=E1102
        comment="最后更新时间",  # 添加字段注释
    )
    created_at: MappedColumn[datetime] = mapped_column(
        DateTime,
        nullable=False,
        insert_default=func.now(),  # pylint: disable=E1102
        comment="创建时间",  # 添加字段注释
    )

    __table_args__ = (
        Index("idx_alpha_id_data_field", "alpha_id", "data_field", unique=True),
    )
