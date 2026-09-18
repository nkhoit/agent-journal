"""Privileged Linux harness: drop a child identity, never change host accounts."""

import errno
import json
import os
import subprocess
import sys
import unittest

from s4_bootstrap_test import BootstrapTest, PROCESS_TIMEOUT


class ForeignUidTest(BootstrapTest):
    def setUp(self):
        self.assertTrue(sys.platform.startswith("linux"), "this harness requires Linux")
        self.assertEqual(os.geteuid(), 0, "run this dedicated harness as root; denial was not tested")
        super().setUp()

    def test_foreign_uid_socket_denied(self):
        self.assertEqual(self.directory.stat().st_uid, 0)
        self.assertEqual(self.directory.stat().st_mode & 0o777, 0o700)
        self.assertEqual(self.socket.stat().st_uid, 0)
        self.assertEqual(self.socket.stat().st_mode & 0o777, 0o600)
        child = """
import json, os, socket, sys
result = {"uid": os.geteuid(), "gid": os.getegid(), "groups": os.getgroups()}
with socket.socket(socket.AF_UNIX) as connection:
    connection.settimeout(3)
    try:
        connection.connect(sys.argv[1])
    except OSError as error:
        result["errno"] = error.errno
    else:
        result["errno"] = 0
print(json.dumps(result))
"""
        result = subprocess.run(
            [sys.executable, "-c", child, str(self.socket)],
            user=65534, group=65534, extra_groups=[],
            capture_output=True, timeout=PROCESS_TIMEOUT, check=True,
        )
        evidence = json.loads(result.stdout)
        self.assertEqual(evidence, {"uid": 65534, "gid": 65534, "groups": [], "errno": errno.EACCES})
        self.assertEqual(result.stderr, b"")
        self.admin("principal-create", "authorized-example", "Authorized")
        self.assertEqual(self.directory.stat().st_mode & 0o777, 0o700)
        self.assertEqual(self.socket.stat().st_mode & 0o777, 0o600)


if __name__ == "__main__":
    if not sys.platform.startswith("linux") or os.geteuid() != 0:
        print("Foreign-UID denial requires privileged Linux execution; no test was run.", file=sys.stderr)
        sys.exit(2)
    suite = unittest.TestSuite([ForeignUidTest("test_foreign_uid_socket_denied")])
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    sys.exit(not result.wasSuccessful())
