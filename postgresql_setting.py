#!/usr/bin/python
# -*- coding: utf-8 -*-

ANSIBLE_METADATA = {'status': ['stableinterface'],
                    'supported_by': 'community',
                    'version': '1.0'}


DOCUMENTATION = '''
---
module: postgresql_setting
short_description: manage config settings for PostgreSQL instance.
description:
  - Change server configuration parameters across the entire database cluster
  - New values will be effective after the next server configuration reload,
    or after the next server restart in the case of parameters that can only
    be changed at server start
  - Only superusers can change configuration settings
author: "Kostiantyn Nemchenko (@kostiantyn-nemchenko)"
version_added: "2.3"
requirements:
  - psycopg2
options:
  login_user:
    description:
      - The username used to authenticate with
    required: false
    default: null
  login_password:
    description:
      - The password used to authenticate with
    required: false
    default: null
  login_host:
    description:
      - Host running the database
    required: false
    default: localhost
  login_unix_socket:
    description:
      - Path to a Unix domain socket for local connections
    required: false
    default: null
  port:
    description:
      - Database port to connect to.
    required: false
    default: 5432
  option:
    description:
      - The parameter from PostgreSQL configuration file
    required: true
    default: null
  value:
    description:
      - The value of the parameter to change
    required: false
    default: null
  state:
    description:
      - The parameter state
    required: false
    default: present
    choices: [ "present", "absent" ]
'''


EXAMPLES = '''
# Set work_mem parameter to 8MB
- postgresql_setting:
    guc: work_mem
    value: 8MB
    state: present

# Allow only local TCP/IP "loopback" connections to be made
- postgresql_setting:
    guc: listen_addresses
    state: absent

# Enable autovacuum
- postgresql_setting:
    guc: autovacuum
    value: on
'''
import traceback

try:
    import psycopg2
    import psycopg2.extras
    from psycopg2 import sql
except ImportError:
    postgresqldb_found = False
else:
    postgresqldb_found = True

# import module snippets
from ansible.module_utils.six import iteritems
from ansible.module_utils.basic import AnsibleModule
from ansible.module_utils.database import SQLParseError
from ansible.module_utils._text import to_native


class NotSupportedError(Exception):
    pass


# ===========================================
# PostgreSQL module specific support methods.
#

def is_guc_configurable(cursor, guc):
    """Check if guc is a preset parameter
    https://www.postgresql.org/docs/current/static/runtime-config-preset.html
    """
    cursor.execute("""
        SELECT EXISTS
            (SELECT 1
             FROM pg_settings
             WHERE context <> 'internal'
             AND name = %s);
        """,
        (guc,)
    )
    return cursor.fetchone()[0]


def get_default_guc_value(cursor, guc):
    """Get parameter value assumed at server startup"""
    cursor.execute("""
        SELECT boot_val
        FROM pg_settings
        WHERE name = %s;
        """,
        (guc,)
    )
    return cursor.fetchone()[0]


def is_guc_default(cursor, guc):
    """Whether the parameter has not been changed since the last database start or
    configuration reload"""
    cursor.execute("""
        SELECT EXISTS
            (SELECT 1
             FROM pg_settings
             WHERE boot_val = reset_val
             AND name = %s);
        """,
        (guc,)
    )
    return cursor.fetchone()[0]


def guc_exists(cursor, guc):
    """Check if such parameter exists"""
    cursor.execute("""
        SELECT name
        FROM pg_settings
        WHERE name = %s;
        """,
        (guc,)
    )
    return cursor.rowcount > 0


def do_guc_reset(cursor, guc):
    """Reset parameter if it has non-default value"""
    if not is_guc_default(cursor, guc):
        cursor.execute(
            sql.SQL("ALTER SYSTEM RESET {}").format(sql.Identifier(guc)))
        return True
    else:
        return False


def do_guc_set(cursor, guc, value):
    """Set new value for parameter"""
    if not guc_matches(cursor, guc, value):
        cursor.execute(
            sql.SQL("ALTER SYSTEM SET {} TO %s").format(sql.Identifier(guc)),  
            (value,))
        return True
    else:
        return False


def guc_matches(cursor, guc, value):
    """Check if setting matches the specified value"""
    cursor.execute("SELECT current_setting(%s) = %s", (guc, value))
    return cursor.fetchone()[0]

# ===========================================
# Module execution.
#


def main():
    module = AnsibleModule(
        argument_spec=dict(
            login_user=dict(default="postgres"),
            login_password=dict(default="", no_log=True),
            login_host=dict(default=""),
            login_unix_socket=dict(default=""),
            port=dict(default="5432"),
            guc=dict(required=True,
                     aliases=["name", "setting", "option", "parameter"]),
            value=dict(default=""),
            state=dict(default="present", choices=["absent", "present"]),
        ),
        supports_check_mode=True
    )

    if not postgresqldb_found:
        module.fail_json(msg="the python psycopg2 module is required")

    guc = module.params["guc"]
    value = module.params["value"]
    port = module.params["port"]
    state = module.params["state"]
    changed = False

    # To use defaults values, keyword arguments must be absent, so
    # check which values are empty and don't include in the **kw
    # dictionary
    params_map = {
        "login_host": "host",
        "login_user": "user",
        "login_password": "password",
        "port": "port"
    }
    kw = dict((params_map[k], v) for (k, v) in iteritems(module.params)
              if k in params_map and v != '')

    # If a login_unix_socket is specified, incorporate it here.
    is_localhost = "host" not in kw or kw["host"] == "" or kw["host"] == "localhost"
    
    if is_localhost and module.params["login_unix_socket"] != "":
        kw["host"] = module.params["login_unix_socket"]

    try:
        db_connection = psycopg2.connect(database="postgres", **kw)
        # Enable autocommit
        if psycopg2.__version__ >= '2.4.2':
            db_connection.autocommit = True
        else:
            db_connection.set_isolation_level(psycopg2
                                              .extensions
                                              .ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = db_connection.cursor(
                    cursor_factory=psycopg2.extras.DictCursor)
    except Exception as e:
        module.fail_json(msg="unable to connect to database: %s" % to_native(e), 
                         exception=traceback.format_exc())

    try:
        if is_guc_configurable(cursor, guc):
            if module.check_mode:
                if state == "absent":
                    changed = not is_guc_default(cursor, guc)
                elif state == "present":
                    changed = not guc_matches(cursor, guc, value)
                module.exit_json(changed=changed, guc=guc)

            if state == "absent":
                try:
                    changed = do_guc_reset(cursor, guc)
                except SQLParseError as e:
                    e = get_exception()
                    module.fail_json(msg=to_native(e), exception=traceback.format_exc())

            elif state == "present":
                try:
                    changed = do_guc_set(cursor, guc, value)
                except SQLParseError as e:
                    e = get_exception()
                    module.fail_json(msg=to_native(e), exception=traceback.format_exc())
        else:
            module.warn("Guc %s does not exist or is preset" % guc)
    except NotSupportedError as e:
        module.fail_json(msg=to_native(e), exception=traceback.format_exc())
    except SystemExit:
        # Avoid catching this on Python 2.4
        raise
    except Exception as e:
        module.fail_json(msg="Database query failed: %s" % to_native(e), exception=traceback.format_exc())

    module.exit_json(changed=changed, guc=guc)


if __name__ == '__main__':
    main()

