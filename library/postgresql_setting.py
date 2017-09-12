#!/usr/bin/python
# -*- coding: utf-8 -*-

ANSIBLE_METADATA = {'status': ['stableinterface'],
                    'supported_by': 'community',
                    'version': '1.0'}

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    postgresqldb_found = False
else:
    postgresqldb_found = True
from ansible.module_utils.six import iteritems

class NotSupportedError(Exception):
    pass


# ===========================================
# PostgreSQL module specific support methods.
#

def option_ispreset(cursor, option):
    """Check if option is a preset parameter 
    https://www.postgresql.org/docs/current/static/runtime-config-preset.html
    """
    cursor.execute("SELECT EXISTS(SELECT 1 FROM pg_settings WHERE context = 'internal' AND name = '%s')" % option)
    return cursor.fetchone()[0]

def option_get_default_value(cursor, option):
    """Get parameter value assumed at server startup"""
    cursor.execute("SELECT boot_val FROM pg_settings WHERE name = '%s'" % option)
    return cursor.fetchone()[0]

def option_isdefault(cursor, option):
    """Whether the parameter has not been changed since the last database start or configuration reload"""
    cursor.execute("SELECT boot_val, reset_val FROM pg_settings WHERE name = '%s'" % option)
    rows = cursor.fetchone()
    if cursor.rowcount > 0:
        default_value, current_value = rows[0], rows[1]
        return default_value == current_value
    else:
        return False

def option_exists(cursor, option):
    """Check if such parameter exists"""
    cursor.execute("SELECT name FROM pg_settings WHERE name = '%s'" % option)
    return cursor.rowcount > 0

def option_reset(cursor, option):
    """Reset parameter if it has non-default value"""
    if not option_isdefault(cursor, option):
        cursor.execute("ALTER SYSTEM SET %s TO %s" % (option, option_get_default_value(cursor, option)))
        return True
    else:
        return False

def option_set(cursor, option, value):
    """Set new value for parameter"""
    if not option_matches(cursor, option, value):
        cursor.execute("ALTER SYSTEM SET %s TO '%s'" % (option, value))
        return True
    else:
        return False

def option_matches(cursor, option, value):
    """Check if setting matches the specified value"""
    cursor.execute("SELECT current_setting('%s') = '%s'" % (option, value))
    return cursor.fetchone()[0]

# ===========================================
# Module execution.
#

def main():
    module = AnsibleModule(
        argument_spec=dict(
            login_user=dict(default="postgres"),
            login_password=dict(default=""),
            login_host=dict(default=""),
            login_unix_socket=dict(default=""),
            port=dict(default="5432"),
            option=dict(required=True, aliases=['name', 'setting', 'guc', 'parameter']),
            value=dict(default=""),
            state=dict(default="present", choices=["absent", "present"]),
        ),
        supports_check_mode = True
    )

    if not postgresqldb_found:
        module.fail_json(msg="the python psycopg2 module is required")

    option = module.params["option"]
    value = module.params["value"]
    port = module.params["port"]
    state = module.params["state"]
    changed = False

    # To use defaults values, keyword arguments must be absent, so
    # check which values are empty and don't include in the **kw
    # dictionary
    params_map = {
        "login_host":"host",
        "login_user":"user",
        "login_password":"password",
        "port":"port"
    }
    kw = dict( (params_map[k], v) for (k, v) in iteritems(module.params)
              if k in params_map and v != '' )

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
    except Exception:
        e = get_exception()
        module.fail_json(msg="unable to connect to database: %s" % e)

    try:
        if option_ispreset(cursor, option):
            module.warn("Option %s is preset, so it can only be set at initdb or before building from source code. "
                        "For details, see https://www.postgresql.org/docs/current/static/runtime-config-preset.html" % option)
        elif option_exists(cursor, option):
            if module.check_mode:
                if state == "absent":
                    changed = not option_isdefault(cursor, option)
                elif state == "present":
                    changed = not option_matches(cursor, option, value)
                module.exit_json(changed=changed, option=option)

            if state == "absent":
                try:
                    changed = option_reset(cursor, option)
                except SQLParseError:
                    e = get_exception()
                    module.fail_json(msg=str(e))

            elif state == "present":
                try:
                    changed = option_set(cursor, option, value)
                except SQLParseError:
                    e = get_exception()
                    module.fail_json(msg=str(e))
        else:
            module.warn("Option %s does not exist" % option)
    except NotSupportedError:
        e = get_exception()
        module.fail_json(msg=str(e))
    except SystemExit:
        # Avoid catching this on Python 2.4 
        raise
    except Exception:
        e = get_exception()
        module.fail_json(msg="Database query failed: %s" % e)

    module.exit_json(changed=changed, option=option)

# import module snippets
from ansible.module_utils.basic import *
from ansible.module_utils.database import *
if __name__ == '__main__':
    main()