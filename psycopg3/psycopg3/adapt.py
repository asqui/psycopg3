"""
Entry point into the adaptation system.
"""

# Copyright (C) 2020 The Psycopg Team

from typing import Any, cast, Callable, Optional, Type, Union

from . import pq
from . import proto
from .pq import Format as Format
from .oids import builtins
from .proto import AdaptContext, DumpersMap, DumperType, LoadersMap, LoaderType
from .cursor import BaseCursor
from .connection import BaseConnection

TEXT_OID = builtins["text"].oid


class Dumper:
    """
    Convert Python object of the type *src* to PostgreSQL representation.
    """

    globals: DumpersMap = {}
    connection: Optional[BaseConnection]

    def __init__(self, src: type, context: AdaptContext = None):
        self.src = src
        self.context = context
        self.connection = _connection_from_context(context)

    def dump(self, obj: Any) -> bytes:
        """Convert the object *obj* to PostgreSQL representation."""
        raise NotImplementedError()

    def quote(self, obj: Any) -> bytes:
        """Convert the object *obj* to escaped representation."""
        value = self.dump(obj)

        if self.connection:
            esc = pq.Escaping(self.connection.pgconn)
            return esc.escape_literal(value)
        else:
            esc = pq.Escaping()
            return b"'%s'" % esc.escape_string(value)

    @property
    def oid(self) -> int:
        """The oid to pass to the server, if known."""
        return 0

    @classmethod
    def register(
        cls,
        src: Union[type, str],
        context: AdaptContext = None,
        format: Format = Format.TEXT,
    ) -> None:
        """
        Configure *context* to use this dumper to convert object of type *src*.
        """
        if not isinstance(src, (str, type)):
            raise TypeError(
                f"dumpers should be registered on classes, got {src} instead"
            )

        where = context.dumpers if context else Dumper.globals
        where[src, format] = cls

    @classmethod
    def register_binary(
        cls, src: Union[type, str], context: AdaptContext = None
    ) -> None:
        """
        Configure *context* to use this dumper for binary format conversion.
        """
        cls.register(src, context, format=Format.BINARY)

    @classmethod
    def text(cls, src: Union[type, str]) -> Callable[[DumperType], DumperType]:
        def text_(dumper: DumperType) -> DumperType:
            dumper.register(src)
            return dumper

        return text_

    @classmethod
    def binary(
        cls, src: Union[type, str]
    ) -> Callable[[DumperType], DumperType]:
        def binary_(dumper: DumperType) -> DumperType:
            dumper.register_binary(src)
            return dumper

        return binary_


class Loader:
    """
    Convert PostgreSQL objects with OID *oid* to Python objects.
    """

    globals: LoadersMap = {}
    connection: Optional[BaseConnection]

    def __init__(self, oid: int, context: AdaptContext = None):
        self.oid = oid
        self.context = context
        self.connection = _connection_from_context(context)

    def load(self, data: bytes) -> Any:
        """Convert a PostgreSQL value to a Python object."""
        raise NotImplementedError()

    @classmethod
    def register(
        cls,
        oid: int,
        context: AdaptContext = None,
        format: Format = Format.TEXT,
    ) -> None:
        """
        Configure *context* to use this loader to convert values with OID *oid*.
        """
        if not isinstance(oid, int):
            raise TypeError(
                f"loaders should be registered on oid, got {oid} instead"
            )

        where = context.loaders if context else Loader.globals
        where[oid, format] = cls

    @classmethod
    def register_binary(cls, oid: int, context: AdaptContext = None) -> None:
        """
        Configure *context* to use this loader to convert binary values.
        """
        cls.register(oid, context, format=Format.BINARY)

    @classmethod
    def text(cls, oid: int) -> Callable[[LoaderType], LoaderType]:
        def text_(loader: LoaderType) -> LoaderType:
            loader.register(oid)
            return loader

        return text_

    @classmethod
    def binary(cls, oid: int) -> Callable[[LoaderType], LoaderType]:
        def binary_(loader: LoaderType) -> LoaderType:
            loader.register_binary(oid)
            return loader

        return binary_


def _connection_from_context(
    context: AdaptContext,
) -> Optional[BaseConnection]:
    if not context:
        return None
    elif isinstance(context, BaseConnection):
        return context
    elif isinstance(context, BaseCursor):
        return cast(BaseConnection, context.connection)
    elif isinstance(context, Transformer):
        return context.connection
    else:
        raise TypeError(f"can't get a connection from {type(context)}")


Transformer: Type[proto.Transformer]

# Override it with fast object if available
if pq.__impl__ == "c":
    from psycopg3_c import _psycopg3

    Transformer = _psycopg3.Transformer
else:
    from . import _transform

    Transformer = _transform.Transformer
