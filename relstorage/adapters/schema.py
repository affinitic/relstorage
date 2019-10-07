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
import abc
import six

from collections import namedtuple
import logging
from functools import partial

from ZODB.POSException import StorageError
from ._util import query_property
from ._util import noop_when_history_free

log = logging.getLogger("relstorage")
tmpl_property = partial(query_property,
                        property_suffix='_TMPLS',
                        lazy_suffix='_TMPL')

ResultDescription = namedtuple(
    'ResultDescription',
    # First two are mandatory, remaining five may be None
    # Example:
    # ('Name', 253, 17, 192, 192, 0, 0),
    ('name', 'type_code', 'display_size',
     'internal_size', 'precision', 'scale', 'null_ok'))


@six.add_metaclass(abc.ABCMeta)
class AbstractSchemaInstaller(object):

    # Keep this list in the same order as the schema scripts
    # These must be lower case, and all queries we write must
    # use lower case table names (MySQL has weird casing rules,
    # but this is also how we do comparisons in Python on table metadata.)

    all_tables = (
        'commit_lock',
        'pack_lock',
        'transaction',
        'new_oid',
        'object_state',
        'blob_chunk',
        'current_object',
        'object_ref',
        'object_refs_added',
        'pack_object',
        'pack_state',
        'pack_state_tid',
        'temp_store',
        'temp_blob_chunk',
        'temp_pack_visit',
        'temp_undo',
    )
    #: The type of the column used to hold transaction IDs
    #: and object IDs (64-bit integers).
    COLTYPE_OID_TID = 'BIGINT'
    #: The type of the column used to hold binary strings.
    #: Our default is appropriate for PostgreSQL.
    COLTYPE_BINARY_STRING = 'BYTEA'
    COLTYPE_STATE = COLTYPE_BINARY_STRING
    #: The type of the column used to number blob chunks.
    COLTYPE_BLOB_CHUNK_NUM = 'BIGINT'
    #: The type of the column used to store blob chunks.
    COLTYPE_BLOB_CHUNK = 'OID'
    #: The type of the column used to store MD5 hash strings.
    COLTYPE_MD5 = 'CHAR(32)'

    database_type = None  # provided by a subclass

    def __init__(self, connmanager, runner, keep_history):
        self.connmanager = connmanager
        self.keep_history = keep_history
        self.runner = runner.with_format_vars(
            tid_type=self.COLTYPE_OID_TID,
            oid_type=self.COLTYPE_OID_TID,
            binary_string_type=self.COLTYPE_BINARY_STRING,
            blob_chunk_type=self.COLTYPE_BLOB_CHUNK,
            blob_chunk_num_type=self.COLTYPE_BLOB_CHUNK_NUM,
            md5_type=self.COLTYPE_MD5,
            state_type=self.COLTYPE_STATE,
            transactional_suffix=self.TRANSACTIONAL_TABLE_SUFFIX,
        )

    def _metadata_to_native_str(self, value):
        # Some drivers, in some configurations, notably older versions
        # of MySQLdb (mysqlclient) on Python 3 in 'NAMES binary' mode,
        # can return column names and the like as bytes when we want native str.
        # pg8000 on Python2 does the reverse and returns unicode when we want
        # native str.
        if value is not None and not isinstance(value, str):
            # Checking for bytes tells us that it's a unicode object on Python 2;
            # we won't get here if it's bytes (because bytes is str)
            value = value.decode('ascii') if isinstance(value, bytes) else value.encode('ascii"')
        return value

    def _column_descriptions(self, cursor):
        __traceback_info__ = cursor.description
        return [ResultDescription(self._metadata_to_native_str(r[0]),
                                  # Not all drivers return lists or tuples
                                  # or things that can be sliced; psycopg2/cffi returns
                                  # an arbitrary sequence.
                                  # MySqlConnector-Python has been observed to provide
                                  # extra attributes.
                                  *list(r)[1:7])
                for r in cursor.description]

    def _rows_as_dicts(self, cursor):
        """
        An iterator of the rows as dictionaries, named by the
        lower-case column name.
        Some drivers offer the ability to do this directly when
        the statement is executed or the cursor is created;
        this is a lowest-common denominator way to do it utilizing
        DB-API 2.0 attributes.
        """
        column_descrs = self._column_descriptions(cursor)
        for row in cursor:
            result = {
                column_descr.name.lower(): column_value
                for column_descr, column_value in zip(column_descrs, row)
            }
            yield result

    @abc.abstractmethod
    def list_tables(self, cursor):
        raise NotImplementedError()

    @abc.abstractmethod
    def list_sequences(self, cursor):
        raise NotImplementedError()

    @abc.abstractmethod
    def get_database_name(self, cursor):
        raise NotImplementedError()

    @abc.abstractmethod
    def _create_commit_lock(self, cursor):
        """
        Create the global lock held during commit.

        (MySQL and PostgreSQL do this differently.)
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def _create_pack_lock(self, cursor):
        """
        Create the global lock held during pack.

        (MySQL and PostgreSQL do this differently.)
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def _create_transaction(self, cursor):
        """
        The transaction table lists all the transactions in the database.

        This table is only used for history-preserving databases.
        """
        self.runner.run_script(cursor, self.CREATE_TRANSACTION_STMT_TMPL)


    @abc.abstractmethod
    def _create_new_oid(self, cursor):
        """
        Create the incrementing sequence for new OIDs.

        This should be the same for history free and preserving
        schemas.
        """
        raise NotImplementedError()

    CREATE_OBJECT_STATE_TMPLS = (
        """
        CREATE TABLE object_state (
            zoid        {oid_type} NOT NULL,
            tid         {tid_type} NOT NULL
                           REFERENCES transaction,
            prev_tid    {tid_type} NOT NULL
                           REFERENCES transaction,
            md5         {md5_type},
            state_size  BIGINT NOT NULL,
            state       {state_type},
            CONSTRAINT object_state_pk
                PRIMARY KEY (zoid, tid),
            CHECK (tid > 0),
            CHECK (state_size >= 0)
        ) {transactional_suffix};
        CREATE INDEX object_state_tid ON object_state (tid);
        CREATE INDEX object_state_prev_tid ON object_state (prev_tid);
        """,
        """
        CREATE TABLE object_state (
            zoid        {oid_type} NOT NULL PRIMARY KEY,
            tid         {tid_type} NOT NULL,
            state_size  BIGINT NOT NULL,
            state       {state_type} NOT NULL,
            CHECK (tid > 0),
            CHECK (state_size >= 0)
        ) {transactional_suffix};
        CREATE INDEX object_state_tid ON object_state (tid);
        """
    )

    CREATE_OBJECT_STATE_TMPL = tmpl_property('CREATE_OBJECT_STATE')

    def _create_object_state(self, cursor):
        """
        Create the table holding all object states for all transactions.

        If the schema is history-free, only store the current state.
        History-preserving schemas may have a NULL `object_state` to represent
        uncreation.
        """
        self.runner.run_script(cursor, self.CREATE_OBJECT_STATE_TMPL)

    CREATE_BLOB_CHUNK_TMPLS = (
        """
        CREATE TABLE blob_chunk (
            zoid        {oid_type} NOT NULL,
            tid         {tid_type} NOT NULL,
            chunk_num   {blob_chunk_num_type} NOT NULL,
            chunk       {blob_chunk_type} NOT NULL,
            CONSTRAINT blob_chunk_pk
                PRIMARY KEY (zoid, tid, chunk_num),
            CONSTRAINT blob_chunk_fk
                FOREIGN KEY (zoid, tid)
                REFERENCES object_state (zoid, tid)
                ON DELETE CASCADE
        ) {transactional_suffix};
        CREATE INDEX blob_chunk_lookup ON blob_chunk (zoid, tid);
        """,
        """
        CREATE TABLE blob_chunk (
            zoid        {oid_type} NOT NULL,
            chunk_num   {blob_chunk_num_type} NOT NULL,
            tid         {tid_type} NOT NULL,
            chunk       {blob_chunk_type} NOT NULL,
            CONSTRAINT blob_chunk_pk
                PRIMARY KEY (zoid, chunk_num),
            CONSTRAINT  blob_chunk_fk
                FOREIGN KEY (zoid)
                REFERENCES object_state (zoid)
                ON DELETE CASCADE
        ) {transactional_suffix};
        CREATE INDEX blob_chunk_lookup ON blob_chunk (zoid);
        """
    )

    CREATE_BLOB_CHUNK_TMPL = tmpl_property('CREATE_BLOB_CHUNK')

    def _create_blob_chunk(self, cursor):
        """
        Create the table holding all blob states for all transactions.

        If the schema is history-free, only store the current state.
        """
        self.runner.run_script(cursor, self.CREATE_BLOB_CHUNK_TMPL)

    CREATE_CURRENT_OBJECT_TMPL = """
    CREATE TABLE current_object (
        zoid        {oid_type} NOT NULL PRIMARY KEY,
        tid         {tid_type} NOT NULL,
        FOREIGN KEY (zoid, tid)
            REFERENCES object_state (zoid, tid)
    ) {transactional_suffix};
    CREATE INDEX current_object_tid ON current_object (tid);
    """

    def _create_current_object(self, cursor):
        """
        Table that stores pointers to the current object state.

        This table is only used for history-preserving databases.
        """
        self.runner.run_script(cursor, self.CREATE_CURRENT_OBJECT_TMPL)

    CREATE_OBJECT_REF_TMPLS = (
        """
        CREATE TABLE object_ref (
            zoid        {oid_type} NOT NULL,
            tid         {tid_type} NOT NULL,
            to_zoid     {oid_type} NOT NULL,
            PRIMARY KEY (tid, zoid, to_zoid)
        ) {transactional_suffix};
        """,
        """
        CREATE TABLE object_ref (
            zoid        {oid_type} NOT NULL,
            to_zoid     {tid_type} NOT NULL,
            tid         {oid_type} NOT NULL,
            PRIMARY KEY (zoid, to_zoid)
        ) {transactional_suffix};
        """
    )

    CREATE_OBJECT_REF_TMPL = tmpl_property('CREATE_OBJECT_REF')

    def _create_object_ref(self, cursor):
        """
        A list of referenced OIDs from each object_state. This
        table is populated as needed during packing. To prevent unnecessary
        table locking, it does not use foreign keys, which is safe because
        rows in object_state are never modified once committed, and rows are
        removed from object_state only by packing.
        """
        self.runner.run_script(cursor, self.CREATE_OBJECT_REF_TMPL)

    CREATE_OBJECT_REFS_ADDED_TMPLS = (
        """
        CREATE TABLE object_refs_added (
            tid         {tid_type} NOT NULL PRIMARY KEY
        ) {transactional_suffix};
        """,
        """
        CREATE TABLE object_refs_added (
            zoid        {oid_type} NOT NULL PRIMARY KEY,
            tid         {tid_type} NOT NULL
        ) {transactional_suffix}
        """
    )

    CREATE_OBJECT_REFS_ADDED_TMPL = tmpl_property('CREATE_OBJECT_REFS_ADDED')

    def _create_object_refs_added(self, cursor):
        """
        The object_refs_added table tracks whether object_refs has been
        populated for all states in a given transaction. An entry is added
        only when the work is finished. To prevent unnecessary table locking,
        it does not use foreign keys, which is safe because object states are
        never added to a transaction once committed, and rows are removed
        from the transaction table only by packing.
        """
        self.runner.run_script(cursor, self.CREATE_OBJECT_REFS_ADDED_TMPL)

    CREATE_PACK_OBJECT_TMPL = """
    CREATE TABLE pack_object (
        zoid        {oid_type} NOT NULL PRIMARY KEY,
        keep        BOOLEAN NOT NULL,
        keep_tid    {oid_type} NOT NULL,
        visited     BOOLEAN NOT NULL DEFAULT FALSE
    ) {transactional_suffix};
    """

    CREATE_PACK_OBJECT_IX_TMPL = """
    CREATE INDEX pack_object_keep_zoid
    ON pack_object (keep, zoid);
    """

    def _create_pack_object(self, cursor):
        """
        pack_object contains temporary state during garbage collection: The
        list of all objects, a flag signifying whether the object should be
        kept, and a flag signifying whether the object's references have been
        visited. The keep_tid field specifies the current revision of the
        object.
        """
        self.runner.run_script(
            cursor,
            self.CREATE_PACK_OBJECT_TMPL + self.CREATE_PACK_OBJECT_IX_TMPL,
        )

    CREATE_PACK_STATE_TMPL = """
    CREATE TABLE pack_state (
        tid         {tid_type} NOT NULL,
        zoid        {oid_type} NOT NULL,
        PRIMARY KEY (tid, zoid)
    ) {transactional_suffix};
    """

    @noop_when_history_free
    def _create_pack_state(self, cursor):
        """
        Temporary state during packing: the list of object states
        # to pack.

        This is only used in history-preserving databases.
        """
        self.runner.run_script(cursor, self.CREATE_PACK_STATE_TMPL)

    CREATE_PACK_STATE_TID_TMPL = """
    CREATE TABLE pack_state_tid (
        tid {tid_type} NOT NULL PRIMARY KEY
    ) {transactional_suffix};
    """

    @noop_when_history_free
    def _create_pack_state_tid(self, cursor):
        """
        Temporary state during packing: the list of
        transactions that have at least one object state to pack.

        This is only used in history-preserving databases.
        """
        self.runner.run_script(cursor, self.CREATE_PACK_STATE_TID_TMPL)


    # Most databases handle temp tables on a session-by-session
    # basis.
    def _create_temp_store(self, _cursor):
        """States that will soon be stored."""
        return

    def _create_temp_blob_chunk(self, _cursor):
        """
        Temporary state during packing: a list of objects
        whose references need to be examined.
        """
        return

    def _create_temp_pack_visit(self, _cursor):
        return

    def _create_temp_undo(self, _cursor):
        """
        Temporary state during undo: a list of objects
        to be undone and the tid of the undone state.
        """
        return

    def _init_after_create(self, cursor):
        """
        Create a special '0' transaction to represent object creation. The
        '0' transaction is often referenced by object_state.prev_tid, but
        never by object_state.tid.

        Only in history-preserving databases.
        """
        if self.keep_history:
            stmt = """
            INSERT INTO transaction (tid, username, description)
            VALUES (0, 'system', 'special transaction for object creation');
            """
            self.runner.run_script(cursor, stmt)

    @abc.abstractmethod
    def _reset_oid(self, cursor):
        raise NotImplementedError()

    def create(self, cursor):
        """Create the database tables."""
        for table in self.all_tables:
            meth = getattr(self, '_create_' + table)
            meth(cursor)

        self._init_after_create(cursor)

        tables = self.list_tables(cursor)
        self.check_compatibility(cursor, tables)

    def prepare(self):
        # XXX: We can generalize this to handle triggers, procs, etc,
        # to make subclasses have easier time.
        def callback(_conn, cursor):
            tables = self.list_tables(cursor)
            if 'object_state' not in tables:
                self.create(cursor)
            else:
                self.check_compatibility(cursor, tables)
                self.update_schema(cursor, tables)
        self.connmanager.open_and_call(callback)

    def verify(self):
        self.connmanager.open_and_call(self._verify)

    def _verify(self, conn, cursor): # pylint:disable=unused-argument
        tables = self.list_tables(cursor)
        self.check_compatibility(cursor, tables)

    def check_compatibility(self, cursor, tables): # pylint:disable=unused-argument
        if self.keep_history:
            if 'transaction' not in tables and 'current_object' not in tables:
                raise StorageError(
                    "Schema mismatch: a history-preserving adapter "
                    "can not connect to a history-free database. "
                    "If you need to convert, use the zodbconvert utility."
                )
        else:
            if 'transaction' in tables and 'current_object' in tables:
                raise StorageError(
                    "Schema mismatch: a history-free adapter "
                    "can not connect to a history-preserving database. "
                    "If you need to convert, use the zodbconvert utility."
                )
        if 'blob_chunk' not in tables:
            raise StorageError(
                "Schema mismatch; please create the blob_chunk tables."
                "See migration instructions for RelStorage 1.5."
            )

    def update_schema(self, cursor, tables):
        pass

    _zap_all_tbl_stmt = 'DELETE FROM %s'

    def zap_all(self, reset_oid=True, slow=False):
        """
        Clear all data out of the database.

        :keyword bool slow: If True (*not* the default) then database
            specific optimizations will be skipped and rows will simply be
            DELETEd. This is helpful when other connections might be open and
            holding some kind of locks.
        """
        stmt = self._zap_all_tbl_stmt if not slow else AbstractSchemaInstaller._zap_all_tbl_stmt

        def callback(_conn, cursor):
            existent = set(self.list_tables(cursor))
            todo = reversed(self.all_tables)
            log.debug("Checking tables: %r", todo)
            for table in todo:
                log.debug("Considering table %s", table)
                if table.startswith('temp_'):
                    continue
                if table in existent:
                    log.debug("Deleting from table %s...", table)
                    cursor.execute(stmt % table)
            log.debug("Done deleting from tables.")

            log.debug("Running init script.")
            self._init_after_create(cursor)
            log.debug("Done running init script.")

            if reset_oid:
                log.debug("Running OID reset script.")
                self._reset_oid(cursor)
                log.debug("Done running OID reset script.")

        self.connmanager.open_and_call(callback)


    def drop_all(self):
        """Drop all tables and sequences."""
        def callback(_conn, cursor):
            existent = set(self.list_tables(cursor))
            todo = list(self.all_tables)
            todo.reverse()
            for table in todo:
                if table in existent:
                    cursor.execute("DROP TABLE %s" % table)
            for sequence in self.list_sequences(cursor):
                cursor.execute("DROP SEQUENCE %s" % sequence)
        self.connmanager.open_and_call(callback)
