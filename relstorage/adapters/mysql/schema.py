##############################################################################
#
# Copyright (c) 2009 Zope Foundation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
##############################################################################
"""
Database schema installers
"""
from __future__ import absolute_import

from ..interfaces import ISchemaInstaller
from ..schema import AbstractSchemaInstaller
from relstorage._compat import db_binary_to_bytes

from ZODB.POSException import StorageError
from zope.interface import implementer


@implementer(ISchemaInstaller)
class MySQLSchemaInstaller(AbstractSchemaInstaller):

    database_type = 'mysql'
    COLTYPE_BINARY_STRING = 'BLOB'
    TRANSACTIONAL_TABLE_SUFFIX = 'ENGINE = InnoDB'
    COLTYPE_MD5 = 'CHAR(32) CHARACTER SET ascii'
    COLTYPE_STATE = 'LONGBLOB'
    COLTYPE_BLOB_CHUNK = 'LONGBLOB'

    def _to_native_str(self, value):
        # Almost all drivers return CHAR/VARCHAR as
        # bytes or possibly str. mysql connector/python, though,
        # will return them as unicode on Py2 if not properly configured.
        # If properly configured, it will return them as bytearray.
        # This doesn't seem configurable.
        # sigh.
        value = db_binary_to_bytes(value)
        if not isinstance(value, str):
            return value.decode('ascii')
        return value

    def get_database_name(self, cursor):
        cursor.execute("SELECT DATABASE()")
        for (name,) in cursor:
            return self._to_native_str(name)

    def list_tables(self, cursor):
        cursor.execute("SHOW TABLES")
        return [self._to_native_str(name)
                for (name,) in cursor.fetchall()]

    def list_sequences(self, cursor):
        return []

    def check_compatibility(self, cursor, tables):
        super(MySQLSchemaInstaller, self).check_compatibility(cursor, tables)
        stmt = "SHOW TABLE STATUS LIKE 'object_state'"
        cursor.execute(stmt)
        for row in cursor:
            for col_index, col in enumerate(cursor.description):
                if col[0].lower() == 'engine':
                    engine = row[col_index]
                    if not isinstance(engine, str):
                        engine = engine.decode('ascii')
                    if engine.lower() != 'innodb':
                        raise StorageError(
                            "The object_state table must use the InnoDB "
                            "engine, but it is using the %s engine." % engine)

    def _create_commit_lock(self, cursor):
        return

    def _create_pack_lock(self, cursor):
        return


    def _create_transaction(self, cursor):
        if self.keep_history:
            stmt = """
            CREATE TABLE transaction (
                tid         BIGINT NOT NULL PRIMARY KEY,
                packed      BOOLEAN NOT NULL DEFAULT FALSE,
                empty       BOOLEAN NOT NULL DEFAULT FALSE,
                username    BLOB NOT NULL,
                description BLOB NOT NULL,
                extension   BLOB
            ) ENGINE = InnoDB;
            """
            self.runner.run_script(cursor, stmt)

    def _create_new_oid(self, cursor):
        stmt = """
        CREATE TABLE new_oid (
            zoid        BIGINT NOT NULL PRIMARY KEY AUTO_INCREMENT
        ) ENGINE = MyISAM;
        """
        self.runner.run_script(cursor, stmt)

    # Temp tables are created in a session-by-session basis
    def _create_temp_store(self, _cursor):
        return

    def _create_temp_blob_chunk(self, _cursor):
        return

    def _create_temp_pack_visit(self, _cursor):
        return

    def _create_temp_undo(self, _cursor):
        return

    def _init_after_create(self, cursor):
        if self.keep_history:
            stmt = """
            INSERT INTO transaction (tid, username, description)
            VALUES (0, 'system', 'special transaction for object creation');
            """
            self.runner.run_script(cursor, stmt)

    def _reset_oid(self, cursor):
        stmt = "TRUNCATE new_oid;"
        self.runner.run_script(cursor, stmt)
