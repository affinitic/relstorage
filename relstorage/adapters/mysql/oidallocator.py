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
IOIDAllocator implementations.
"""

from __future__ import absolute_import

from ..oidallocator import AbstractOIDAllocator
from ..interfaces import IOIDAllocator
from zope.interface import implementer

from perfmetrics import metricmethod

logger = __import__('logging').getLogger(__name__)

@implementer(IOIDAllocator)
class MySQLOIDAllocator(AbstractOIDAllocator):

    # After this many allocated OIDs should the (unlucky) thread that
    # allocated the one evenly divisible by this number attempt to remove
    # old OIDs.
    garbage_collect_interval = 10001

    # How many OIDs to attempt to delete at any one request. Keeping
    # this on the small side relative to the interval limits the
    # chances of deadlocks (by observation).
    garbage_collect_interactive_size = 1000

    # How many OIDs to attempt to delete if we're not allocating, only
    # garbage collecting.
    garbage_collect_batch_size = 3000

    def __init__(self):
        """
        :param type disconnected_exception: The exception to raise when
           we get an invalid value for ``lastrowid``.
        """
        AbstractOIDAllocator.__init__(self)

    def set_min_oid(self, cursor, oid):
        """Ensure the next OID is at least the given OID."""
        n = (oid + 15) // 16
        # if the table is empty, max(zoid) returns null, which
        # of course fails the comparison and so nothing gets inserted.
        stmt = """
        insert into new_oid (zoid)
        select %s
        where %s > (select coalesce(max(zoid), 0) from new_oid)
        """
        cursor.execute(stmt, (n, n))

    @metricmethod
    def new_oids(self, cursor):
        """Return a sequence of new, unused OIDs."""
        # Generate a new auto_incremented ID. This will never conflict
        # with any other session because generated IDs are guaranteed
        # to be unique. However, the DELETE statement may interfere
        # with other sessions and lead to deadlock; this is true even
        # when using the 'DELETE IGNORE'. The INSERT statement takes out an
        # exclusive lock on the PRIMARY KEY index.
        #
        # Session A: INSERTS 2000. -> lock 2000
        # Session B: INSERTS 3000. -> lock 3000
        # Session B: DELETE < 3000 -> wants lock on 2000; locks everything under.
        # Session A: DELETE < 2000 -> hold lock on 2000; wants to lock things under.
        #
        # ORDER BY zoid (ASC or DESC) just runs into more lock issues reported for
        # individual rows.
        #
        # Using the single-row update example provided by
        # https://dev.mysql.com/doc/refman/5.7/en/innodb-locking-reads.html
        # also runs into deadlocks (naturally).
        #
        # Our solution is to just let rows build up if the delete fails. Eventually
        # a GC, which happens at startup, will occur and hopefully get most of them.
        stmt = "INSERT INTO new_oid VALUES ()"
        cursor.execute(stmt)
        # This is a DB-API extension. Fortunately, all
        # supported drivers implement it. (In the past we used
        # cursor.connection.insert_id(), which was specific to MySQLdb
        # and PyMySQL.)
        # 'SELECT LAST_INSERT_ID()' is the pure-SQL way to do this.
        n = cursor.lastrowid

        # At least in one setup (gevent/umysqldb/pymysql/mysql 5.5)
        # we have observed cursor.lastrowid to be None.
        if n % self.garbage_collect_interval == 0:
            self.garbage_collect_oids(cursor, n)
        return self._oid_range_around(n)

    def garbage_collect_oids(self, cursor, max_value=None):
        # Clean out previously generated OIDs.
        batch_size = self.garbage_collect_interactive_size
        if not max_value:
            batch_size = self.garbage_collect_batch_size
            cursor.execute('SELECT MAX(zoid) FROM new_oid')
            row = cursor.fetchone()
            if row:
                max_value = row[0]
        if not max_value:
            return
        # Delete old OIDs, starting with the oldest.
        stmt = """
        DELETE IGNORE
        FROM new_oid
        WHERE zoid < %s
        ORDER BY zoid ASC
        LIMIT %s
        """
        params = (max_value, batch_size)
        __traceback_info__ = stmt, params
        rowcount = True
        while rowcount:
            try:
                cursor.execute(stmt, params)
            except Exception: # pylint:disable=broad-except
                # Luckily, a deadlock only rolls back the previous statement, not the
                # whole transaction.
                # TODO: We'd prefer to only do this for errcode 1213: Deadlock.
                # MySQLdb raises this as an OperationalError; what do all the other
                # drivers do?
                logger.debug("Failed to garbage collect allocated OIDs", exc_info=True)
                break
            else:
                rowcount = cursor.rowcount
                logger.debug("Garbage collected %s old OIDs less than", rowcount, max_value)

    def reset_oid(self, cursor):
        cursor.execute("TRUNCATE TABLE new_oid")
