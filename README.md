# ansible-module-pgsetting
An Ansible module for configuring PostgreSQL settings

# Examples

```
# Set work_mem parameter to 8MB
- postgresql_setting:
    option: work_mem
    value: 8MB
    state: present

# Allow only local TCP/IP "loopback" connections to be made
- postgresql_setting:
    option: listen_addresses
    state: absent

# Enable autovacuum
- postgresql_setting:
    option: autovacuum
    value: on
```