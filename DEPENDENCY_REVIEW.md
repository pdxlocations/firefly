# Dependency and identity changes

Implemented the review findings:

- Installed vnode 0.2.3, meshdb 0.3.0, mudp 1.6.0, meshtastic 2.7.11,
  XEdDSA 1.2.0, Flask-SocketIO 5.7.0, python-socketio 5.17.0, and
  python-engineio 4.15.0 in `.venv`. cryptography remains 43.0.3 because mudp
  requires `<44`. Removed unused eventlet from requirements and the venv.
- Installed packages are the default. Sibling repositories require
  `FIREFLY_LOCAL_DEPS=1`. All startup entry points require the new minimum
  versions; the probe imports the runtime and reports actual module paths.
- New profiles receive server-generated keys and derived IDs. The edit form
  displays a read-only ID; profile APIs reject client-supplied IDs. Key rotation
  is not part of ordinary editing.
- Startup reconciles old databases before reception, including profiles without
  key files. Migration preserves profile UUIDs, records original IDs and public
  keys, checks collisions, updates stored IDs transactionally, and seeds owner
  records with the correct public keys. History retains original packet identity.
- Missing, corrupt, or replaced keys fail closed after migration. Configuration
  writes are atomic with private file permissions. Migration takes a backup and
  reuses generated keys if interrupted. Sessions reload profiles from SQLite.
- A persistent random session secret replaces the fixed default. Writes require
  CSRF tokens and Socket.IO uses same-origin validation. Channel-key validation
  also applies on edits. Logs no longer include channel key prefixes.
- Fixed hop limit 0 persistence and refreshed vnode configuration when profile
  names/hop limits change. SQLite connections close after transaction contexts.
- README documents Python 3.11–3.14, derived identity, remote upgrades, and API
  changes. Docker and native installations share the automatic migration path.

Validation covers profile creation/edits, identity override rejection, migration
with and without keys, repeated migration, missing/corrupt key failures,
collision rollback, session refresh, CSRF enforcement, signed channel packets,
unsigned larger payloads, PKI-encrypted DMs, and ACK sender identity. Signed
metadata remains unverified on UDP; signature presence is not presented as
proof of verification. Tests use isolated storage and mocked sends, not a live
radio mesh. A copy of this checkout's existing database (three profiles) also
migrated successfully using pip-installed dependencies, without runtime keys.

vnode integration still uses private methods for Firefly's shared receiver and
shared channel storage. Release ranges are bounded to avoid adopting a new
minor vnode/meshdb API automatically; transport regression tests cover the
current integration. Upstream public extension hooks would allow removing
these overrides in a future change.
