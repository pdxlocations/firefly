#!/usr/bin/env python3

import sqlite3
import json
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import os


class Database:
    def __init__(self, db_path: str = "firefly.db"):
        self.db_path = db_path
        self.init_database()

    def init_database(self):
        """Initialize the database with required tables"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("PRAGMA foreign_keys = ON")

            conn.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id TEXT PRIMARY KEY,
                    username TEXT NOT NULL,
                    password_hash TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Profiles table (migrate from JSON file)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS profiles (
                    id TEXT PRIMARY KEY,
                    user_id TEXT,
                    node_id TEXT NOT NULL,
                    long_name TEXT NOT NULL,
                    short_name TEXT NOT NULL,
                    channel TEXT NOT NULL,
                    key TEXT NOT NULL,
                    hop_limit INTEGER DEFAULT 3,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE
                )
            """)
            
            # Nodes table - stores nodeinfo by channel (shared across profiles using same channel)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS nodes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    channel INTEGER NOT NULL,  -- Channel number from generate_hash()
                    node_id TEXT NOT NULL,
                    node_num INTEGER NOT NULL,
                    long_name TEXT,
                    short_name TEXT,
                    macaddr BLOB,
                    hw_model TEXT,
                    role TEXT,
                    public_key BLOB,
                    first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    packet_count INTEGER DEFAULT 1,
                    raw_nodeinfo TEXT,  -- JSON blob of the full nodeinfo payload
                    UNIQUE(channel, node_num)  -- Unique per channel, not per profile
                )
            """)
            
            # Messages table - stores all messages by channel (not by profile)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    message_id TEXT UNIQUE,  -- UUID for web UI
                    packet_id INTEGER,  -- Meshtastic packet ID
                    sender_num INTEGER,
                    sender_display TEXT,
                    content TEXT NOT NULL,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    sender_ip TEXT,
                    direction TEXT DEFAULT 'received',  -- 'sent' or 'received'
                    channel INTEGER NOT NULL,  -- Channel number (from generate_hash)
                    message_type TEXT DEFAULT 'channel',  -- 'channel' or 'dm'
                    reply_packet_id INTEGER,
                    target_node_num INTEGER,
                    owner_profile_id TEXT,
                    hop_limit INTEGER,
                    hop_start INTEGER,
                    rx_snr REAL,
                    rx_rssi INTEGER,
                    ack_requested INTEGER DEFAULT 0,
                    ack_status TEXT,
                    ack_error TEXT,
                    ack_updated_at TIMESTAMP
                )
            """)
            
            # Profile read status table - tracks last seen timestamp for each profile+channel combination
            conn.execute("""
                CREATE TABLE IF NOT EXISTS profile_last_seen (
                    profile_id TEXT NOT NULL,
                    channel INTEGER NOT NULL,
                    last_seen_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (profile_id, channel),
                    FOREIGN KEY (profile_id) REFERENCES profiles (id) ON DELETE CASCADE
                )
            """)
            
            # Add hop_limit column to existing profiles table if it doesn't exist
            try:
                conn.execute("ALTER TABLE profiles ADD COLUMN hop_limit INTEGER DEFAULT 3")
                print("[DB] Added hop_limit column to profiles table")
            except sqlite3.OperationalError:
                # Column already exists, which is fine
                pass

            try:
                conn.execute("ALTER TABLE profiles ADD COLUMN channels_json TEXT")
                print("[DB] Added channels_json column to profiles table")
            except sqlite3.OperationalError:
                pass

            try:
                conn.execute("ALTER TABLE profiles ADD COLUMN user_id TEXT")
                print("[DB] Added user_id column to profiles table")
            except sqlite3.OperationalError:
                pass

            for column_sql, column_name in [
                ("ALTER TABLE messages ADD COLUMN message_type TEXT DEFAULT 'channel'", "message_type"),
                ("ALTER TABLE messages ADD COLUMN reply_packet_id INTEGER", "reply_packet_id"),
                ("ALTER TABLE messages ADD COLUMN target_node_num INTEGER", "target_node_num"),
                ("ALTER TABLE messages ADD COLUMN owner_profile_id TEXT", "owner_profile_id"),
                ("ALTER TABLE messages ADD COLUMN ack_requested INTEGER DEFAULT 0", "ack_requested"),
                ("ALTER TABLE messages ADD COLUMN ack_status TEXT", "ack_status"),
                ("ALTER TABLE messages ADD COLUMN ack_error TEXT", "ack_error"),
                ("ALTER TABLE messages ADD COLUMN ack_updated_at TIMESTAMP", "ack_updated_at"),
            ]:
                try:
                    conn.execute(column_sql)
                    print(f"[DB] Added {column_name} column to messages table")
                except sqlite3.OperationalError:
                    pass
            
            # Create indexes for better performance
            conn.execute("CREATE INDEX IF NOT EXISTS idx_nodes_channel ON nodes (channel)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_nodes_last_seen ON nodes (last_seen DESC)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_nodes_channel_last_seen ON nodes (channel, last_seen DESC)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_messages_channel ON messages (channel)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_messages_type ON messages (message_type)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_messages_owner_profile ON messages (owner_profile_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_messages_target_node ON messages (target_node_num)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_messages_timestamp ON messages (timestamp DESC)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_messages_channel_timestamp ON messages (channel, timestamp DESC)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_profile_last_seen ON profile_last_seen (profile_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_profiles_user_id ON profiles (user_id)")
            conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_users_username_nocase ON users (username COLLATE NOCASE)")
            try:
                conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_profiles_node_id_unique ON profiles (node_id)")
            except sqlite3.IntegrityError as e:
                print(f"[DB] Could not create unique node_id index: {e}")
            
            conn.commit()

    @staticmethod
    def _normalize_channels(channels=None, legacy_channel: str = "", legacy_key: str = "") -> List[Dict]:
        normalized = []

        if isinstance(channels, str):
            try:
                channels = json.loads(channels)
            except Exception:
                channels = None

        if isinstance(channels, list):
            for channel in channels:
                if not isinstance(channel, dict):
                    continue
                name = (channel.get("name") or channel.get("channel") or "").strip()
                key = (channel.get("key") or "").strip()
                if not name or not key:
                    continue
                normalized.append({"name": name, "key": key})

        if not normalized and legacy_channel and legacy_key:
            normalized.append({"name": legacy_channel, "key": legacy_key})

        return normalized

    def migrate_profiles_from_json(self, json_file: str = "profiles.json"):
        """Migrate existing profiles from JSON file to database (only if not already migrated)"""
        if not os.path.exists(json_file):
            return False  # No JSON file to migrate
            
        try:
            # Check if we already have profiles in the database
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute("SELECT COUNT(*) FROM profiles")
                existing_count = cursor.fetchone()[0]
                
                if existing_count > 0:
                    print(f"Database already has {existing_count} profiles, skipping JSON migration")
                    return False
            
            # Load and migrate JSON profiles
            with open(json_file, 'r') as f:
                profiles_data = json.load(f)
                
            if not profiles_data:
                print("No profiles found in JSON file")
                return False
                
            with sqlite3.connect(self.db_path) as conn:
                migrated_count = 0
                for profile_id, profile in profiles_data.items():
                    channels = self._normalize_channels(
                        profile.get("channels"),
                        profile.get("channel", ""),
                        profile.get("key", ""),
                    )
                    primary_channel = channels[0]["name"] if channels else profile.get("channel", "")
                    primary_key = channels[0]["key"] if channels else profile.get("key", "")
                    conn.execute("""
                        INSERT OR IGNORE INTO profiles 
                        (id, node_id, long_name, short_name, channel, key, channels_json, created_at, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        profile_id,
                        profile.get('node_id', ''),
                        profile.get('long_name', ''),
                        profile.get('short_name', ''),
                        primary_channel,
                        primary_key,
                        json.dumps(channels),
                        profile.get('created_at', datetime.now().isoformat()),
                        profile.get('updated_at', datetime.now().isoformat())
                    ))
                    migrated_count += 1
                conn.commit()
                
                print(f"Successfully migrated {migrated_count} profiles from {json_file}")
                
                # Only backup the JSON file after successful migration
                backup_file = f"{json_file}.migrated"
                if not os.path.exists(backup_file):
                    os.rename(json_file, backup_file)
                    print(f"JSON profiles backed up as {backup_file}")
                    
                return True
                
        except Exception as e:
            print(f"Error migrating profiles: {e}")
            return False

    def create_user(self, user_id: str, username: str, password_hash: str) -> bool:
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    """
                    INSERT INTO users (id, username, password_hash)
                    VALUES (?, ?, ?)
                    """,
                    (user_id, username.strip(), password_hash),
                )
                conn.commit()
                return True
        except sqlite3.IntegrityError as e:
            print(f"Error creating user: {e}")
            return False
        except Exception as e:
            print(f"Error creating user: {e}")
            return False

    def get_user_by_username(self, username: str) -> Optional[Dict]:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                """
                SELECT id, username, password_hash, created_at
                FROM users
                WHERE username = ? COLLATE NOCASE
                """,
                (username.strip(),),
            )
            row = cursor.fetchone()
            if not row:
                return None
            return {
                "id": row["id"],
                "username": row["username"],
                "password_hash": row["password_hash"],
                "created_at": row["created_at"],
            }

    def get_user_by_id(self, user_id: str) -> Optional[Dict]:
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                """
                SELECT id, username, password_hash, created_at
                FROM users
                WHERE id = ?
                """,
                (user_id,),
            )
            row = cursor.fetchone()
            if not row:
                return None
            return {
                "id": row["id"],
                "username": row["username"],
                "password_hash": row["password_hash"],
                "created_at": row["created_at"],
            }

    def count_users(self) -> int:
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("SELECT COUNT(*) FROM users")
            return int(cursor.fetchone()[0] or 0)

    def update_user_password(self, user_id: str, password_hash: str) -> bool:
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute(
                    """
                    UPDATE users
                    SET password_hash = ?
                    WHERE id = ?
                    """,
                    (password_hash, user_id),
                )
                conn.commit()
                return cursor.rowcount > 0
        except Exception as e:
            print(f"Error updating user password: {e}")
            return False

    def delete_user(self, user_id: str) -> bool:
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("PRAGMA foreign_keys = ON")
                conn.execute(
                    """
                    DELETE FROM messages
                    WHERE owner_profile_id IN (
                        SELECT id FROM profiles WHERE user_id = ?
                    )
                    """,
                    (user_id,),
                )
                cursor = conn.execute(
                    """
                    DELETE FROM users
                    WHERE id = ?
                    """,
                    (user_id,),
                )
                conn.commit()
                return cursor.rowcount > 0
        except Exception as e:
            print(f"Error deleting user: {e}")
            return False

    def claim_orphan_profiles(self, user_id: str) -> int:
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute(
                    """
                    UPDATE profiles
                    SET user_id = ?
                    WHERE user_id IS NULL
                    """,
                    (user_id,),
                )
                conn.commit()
                return int(cursor.rowcount or 0)
        except Exception as e:
            print(f"Error claiming orphan profiles: {e}")
            return 0

    def node_id_in_use(self, node_id: str, exclude_profile_id: Optional[str] = None) -> bool:
        with sqlite3.connect(self.db_path) as conn:
            if exclude_profile_id:
                cursor = conn.execute(
                    """
                    SELECT 1 FROM profiles
                    WHERE node_id = ? AND id != ?
                    LIMIT 1
                    """,
                    (node_id, exclude_profile_id),
                )
            else:
                cursor = conn.execute(
                    """
                    SELECT 1 FROM profiles
                    WHERE node_id = ?
                    LIMIT 1
                    """,
                    (node_id,),
                )
            return cursor.fetchone() is not None

    def get_all_profiles(self, user_id: Optional[str] = None) -> Dict:
        """Get all profiles as dictionary (compatible with existing code)"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            if user_id:
                cursor = conn.execute("""
                    SELECT id, user_id, node_id, long_name, short_name, channel, key, channels_json, hop_limit,
                           created_at, updated_at
                    FROM profiles
                    WHERE user_id = ?
                    ORDER BY created_at DESC
                """, (user_id,))
            else:
                cursor = conn.execute("""
                    SELECT id, user_id, node_id, long_name, short_name, channel, key, channels_json, hop_limit,
                           created_at, updated_at
                    FROM profiles ORDER BY created_at DESC
                """)
            profiles = {}
            for row in cursor:
                channels = self._normalize_channels(row["channels_json"], row["channel"], row["key"])
                primary_channel = channels[0]["name"] if channels else row["channel"]
                primary_key = channels[0]["key"] if channels else row["key"]
                profiles[row['id']] = {
                    'id': row['id'],
                    'user_id': row['user_id'],
                    'node_id': row['node_id'],
                    'long_name': row['long_name'],
                    'short_name': row['short_name'],
                    'channel': primary_channel,
                    'key': primary_key,
                    'channels': channels,
                    'hop_limit': row['hop_limit'] or 3,  # Default to 3 if null
                    'created_at': row['created_at'],
                    'updated_at': row['updated_at']
                }
            return profiles

    def get_profile(self, profile_id: str, user_id: Optional[str] = None) -> Optional[Dict]:
        """Get a specific profile"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            if user_id:
                cursor = conn.execute("""
                    SELECT id, user_id, node_id, long_name, short_name, channel, key, channels_json, hop_limit,
                           created_at, updated_at
                    FROM profiles
                    WHERE id = ? AND user_id = ?
                """, (profile_id, user_id))
            else:
                cursor = conn.execute("""
                    SELECT id, user_id, node_id, long_name, short_name, channel, key, channels_json, hop_limit,
                           created_at, updated_at
                    FROM profiles WHERE id = ?
                """, (profile_id,))
            row = cursor.fetchone()
            if row:
                channels = self._normalize_channels(row["channels_json"], row["channel"], row["key"])
                primary_channel = channels[0]["name"] if channels else row["channel"]
                primary_key = channels[0]["key"] if channels else row["key"]
                return {
                    'id': row['id'],
                    'user_id': row['user_id'],
                    'node_id': row['node_id'],
                    'long_name': row['long_name'],
                    'short_name': row['short_name'],
                    'channel': primary_channel,
                    'key': primary_key,
                    'channels': channels,
                    'hop_limit': row['hop_limit'] or 3,  # Default to 3 if null
                    'created_at': row['created_at'],
                    'updated_at': row['updated_at']
                }
            return None

    def create_profile(self, profile_id: str, user_id: str, node_id: str, long_name: str,
                      short_name: str, channels: List[Dict], hop_limit: int = 3) -> bool:
        """Create a new profile"""
        # Validate hop_limit range (0-7)
        if not isinstance(hop_limit, int) or hop_limit < 0 or hop_limit > 7:
            hop_limit = 3  # Default to 3 if invalid
        channels = self._normalize_channels(channels)
        if not channels:
            return False
        primary = channels[0]
        if self.node_id_in_use(node_id):
            return False
            
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    INSERT INTO profiles (id, user_id, node_id, long_name, short_name, channel, key, channels_json, hop_limit)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    profile_id,
                    user_id,
                    node_id,
                    long_name,
                    short_name,
                    primary["name"],
                    primary["key"],
                    json.dumps(channels),
                    hop_limit,
                ))
                conn.commit()
                return True
        except sqlite3.IntegrityError as e:
            print(f"Error creating profile: {e}")
            return False
        except Exception as e:
            print(f"Error creating profile: {e}")
            return False

    def update_profile(self, profile_id: str, user_id: str, node_id: str, long_name: str,
                      short_name: str, channels: List[Dict], hop_limit: int = 3) -> bool:
        """Update an existing profile"""
        # Validate hop_limit range (0-7)
        if not isinstance(hop_limit, int) or hop_limit < 0 or hop_limit > 7:
            hop_limit = 3  # Default to 3 if invalid
        channels = self._normalize_channels(channels)
        if not channels:
            return False
        primary = channels[0]
        if self.node_id_in_use(node_id, exclude_profile_id=profile_id):
            return False
            
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute("""
                    UPDATE profiles SET node_id = ?, long_name = ?, short_name = ?, 
                                      channel = ?, key = ?, channels_json = ?, hop_limit = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE id = ? AND user_id = ?
                """, (
                    node_id,
                    long_name,
                    short_name,
                    primary["name"],
                    primary["key"],
                    json.dumps(channels),
                    hop_limit,
                    profile_id,
                    user_id,
                ))
                return cursor.rowcount > 0
        except sqlite3.IntegrityError as e:
            print(f"Error updating profile: {e}")
            return False
        except Exception as e:
            print(f"Error updating profile: {e}")
            return False

    def delete_profile(self, profile_id: str, user_id: str) -> bool:
        """Delete a profile and all associated data"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute("DELETE FROM profiles WHERE id = ? AND user_id = ?", (profile_id, user_id))
                return cursor.rowcount > 0
        except Exception as e:
            print(f"Error deleting profile: {e}")
            return False

    def store_node(self, profile_id: str, node_num: int, node_id: str, 
                   long_name: str = None, short_name: str = None, 
                   macaddr: bytes = None, hw_model: str = None, 
                   role: str = None, public_key: bytes = None, 
                   raw_nodeinfo: str = None) -> bool:
        """Store or update a node seen by a profile"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Check if node exists for this profile
                cursor = conn.execute("""
                    SELECT id, packet_count FROM nodes WHERE profile_id = ? AND node_num = ?
                """, (profile_id, node_num))
                existing = cursor.fetchone()
                
                if existing:
                    # Update existing node
                    conn.execute("""
                        UPDATE nodes SET 
                            node_id = COALESCE(?, node_id),
                            long_name = COALESCE(?, long_name),
                            short_name = COALESCE(?, short_name),
                            macaddr = COALESCE(?, macaddr),
                            hw_model = COALESCE(?, hw_model),
                            role = COALESCE(?, role),
                            public_key = COALESCE(?, public_key),
                            last_seen = CURRENT_TIMESTAMP,
                            packet_count = packet_count + 1,
                            raw_nodeinfo = COALESCE(?, raw_nodeinfo)
                        WHERE id = ?
                    """, (node_id, long_name, short_name, macaddr, hw_model, 
                         role, public_key, raw_nodeinfo, existing[0]))
                else:
                    # Insert new node
                    conn.execute("""
                        INSERT INTO nodes 
                        (profile_id, node_num, node_id, long_name, short_name, 
                         macaddr, hw_model, role, public_key, raw_nodeinfo)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (profile_id, node_num, node_id, long_name, short_name, 
                         macaddr, hw_model, role, public_key, raw_nodeinfo))
                conn.commit()
                return True
        except Exception as e:
            print(f"Error storing node: {e}")
            return False

    # === CHANNEL-BASED NODE METHODS (NEW) ===
    
    def store_node_for_channel(self, channel: int, node_num: int, node_id: str, 
                              long_name: str = None, short_name: str = None, 
                              macaddr: bytes = None, hw_model: str = None, 
                              role: str = None, public_key: bytes = None, 
                              raw_nodeinfo: str = None) -> bool:
        """Store or update a node for a specific channel (accessible to all profiles using that channel)"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Check if node exists for this channel
                cursor = conn.execute("""
                    SELECT id, packet_count FROM nodes WHERE channel = ? AND node_num = ?
                """, (channel, node_num))
                existing = cursor.fetchone()
                
                if existing:
                    # Update existing node
                    conn.execute("""
                        UPDATE nodes SET 
                            node_id = COALESCE(?, node_id),
                            long_name = COALESCE(?, long_name),
                            short_name = COALESCE(?, short_name),
                            macaddr = COALESCE(?, macaddr),
                            hw_model = COALESCE(?, hw_model),
                            role = COALESCE(?, role),
                            public_key = COALESCE(?, public_key),
                            last_seen = CURRENT_TIMESTAMP,
                            packet_count = packet_count + 1,
                            raw_nodeinfo = COALESCE(?, raw_nodeinfo)
                        WHERE id = ?
                    """, (node_id, long_name, short_name, macaddr, hw_model, 
                         role, public_key, raw_nodeinfo, existing[0]))
                else:
                    # Insert new node
                    conn.execute("""
                        INSERT INTO nodes 
                        (channel, node_num, node_id, long_name, short_name, 
                         macaddr, hw_model, role, public_key, raw_nodeinfo)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (channel, node_num, node_id, long_name, short_name, 
                         macaddr, hw_model, role, public_key, raw_nodeinfo))
                conn.commit()
                return True
        except Exception as e:
            print(f"Error storing node for channel: {e}")
            return False
    
    def get_nodes_for_channel(self, channel: int) -> List[Dict]:
        """Get all nodes seen on a specific channel (accessible to all profiles using that channel)"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT node_num, node_id, long_name, short_name, macaddr, 
                       hw_model, role, first_seen, last_seen, packet_count, raw_nodeinfo
                FROM nodes WHERE channel = ? ORDER BY last_seen DESC
            """, (channel,))
            
            nodes = []
            for row in cursor:
                # Ensure node_id is properly formatted as hex
                node_id = row['node_id']
                if not node_id and row['node_num']:
                    # Fallback: create hex node_id from node_num
                    node_id = f"!{hex(row['node_num'])[2:].zfill(8)}"
                elif node_id and not node_id.startswith('!'):
                    # Fix malformed node_id that might be decimal
                    try:
                        # If it's a decimal number, convert to hex format
                        decimal_val = int(node_id)
                        node_id = f"!{hex(decimal_val)[2:].zfill(8)}"
                    except ValueError:
                        # If it's not a number, keep as is
                        pass
                
                node_data = {
                    'node_num': row['node_num'],
                    'node_id': node_id,
                    'long_name': row['long_name'],
                    'short_name': row['short_name'],
                    'hw_model': row['hw_model'],
                    'role': row['role'],
                    'first_seen': row['first_seen'],
                    'last_seen': row['last_seen'],
                    'packet_count': row['packet_count']
                }
                
                # Add raw nodeinfo if available
                if row['raw_nodeinfo']:
                    try:
                        node_data['raw_nodeinfo'] = json.loads(row['raw_nodeinfo'])
                    except:
                        pass
                        
                # Format MAC address if available
                if row['macaddr']:
                    try:
                        mac_bytes = row['macaddr']
                        node_data['macaddr'] = ':'.join(f'{b:02x}' for b in mac_bytes)
                    except:
                        pass
                        
                nodes.append(node_data)
            return nodes
    
    def get_nodes_for_profile_channel(self, profile_id: str) -> List[Dict]:
        """Get nodes for a profile based on its channel (convenience method)"""
        from encryption import generate_hash
        
        # Get the profile's channel
        profile = self.get_profile(profile_id)
        if not profile:
            return []
        
        # Calculate channel number
        channel = generate_hash(profile['channel'], profile['key'])
        
        # Get nodes for that channel
        return self.get_nodes_for_channel(channel)
    
    # === LEGACY PROFILE-BASED NODE METHODS (DEPRECATED) ===
    
    def store_node(self, profile_id: str, node_num: int, node_id: str, 
                   long_name: str = None, short_name: str = None, 
                   macaddr: bytes = None, hw_model: str = None, 
                   role: str = None, public_key: bytes = None, 
                   raw_nodeinfo: str = None) -> bool:
        """DEPRECATED: Store node by profile - use store_node_for_channel() instead"""
        print(f"[DEPRECATED] store_node() called - use store_node_for_channel() instead")
        
        # Get profile to determine channel
        from encryption import generate_hash
        profile = self.get_profile(profile_id)
        if not profile:
            return False
        
        # Calculate channel and delegate to new method
        channel = generate_hash(profile['channel'], profile['key'])
        return self.store_node_for_channel(channel, node_num, node_id, long_name, 
                                          short_name, macaddr, hw_model, role, 
                                          public_key, raw_nodeinfo)
    
    def get_nodes_for_profile(self, profile_id: str) -> List[Dict]:
        """DEPRECATED: Get nodes by profile - use get_nodes_for_profile_channel() instead"""
        print(f"[DEPRECATED] get_nodes_for_profile() called - use get_nodes_for_profile_channel() instead")
        return self.get_nodes_for_profile_channel(profile_id)

    def store_message(self, message_id: str, packet_id: int = None,
                     sender_num: int = None, sender_display: str = None,
                     content: str = None, sender_ip: str = None, direction: str = "received",
                     channel: int = None, message_type: str = "channel", reply_packet_id: int = None, target_node_num: int = None,
                     owner_profile_id: str = None, hop_limit: int = None, hop_start: int = None,
                     rx_snr: float = None, rx_rssi: int = None, ack_requested: bool = False,
                     ack_status: str = None, ack_error: str = None, ack_updated_at: str = None) -> bool:
        """Store a message by channel (accessible to any profile using that channel)"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    INSERT INTO messages 
                    (message_id, packet_id, sender_num, sender_display, 
                     content, sender_ip, direction, channel, message_type, reply_packet_id, target_node_num,
                     owner_profile_id, hop_limit, hop_start, rx_snr, rx_rssi, ack_requested, ack_status, ack_error, ack_updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (message_id, packet_id, sender_num, sender_display,
                     content, sender_ip, direction, channel, message_type, reply_packet_id, target_node_num,
                     owner_profile_id, hop_limit, hop_start, rx_snr, rx_rssi,
                     int(bool(ack_requested)), ack_status, ack_error, ack_updated_at))
                conn.commit()
                return True
        except Exception as e:
            print(f"Error storing message: {e}")
            return False

    # Legacy method - use get_messages_for_channel() instead
    def get_messages_for_profile(self, profile_id: str, limit: int = 100) -> List[Dict]:
        """Legacy method - messages are now stored by channel, not profile"""
        print(f"[DEPRECATED] get_messages_for_profile() called - use get_messages_for_channel() instead")
        return []

    def get_stats(self, profile_id: str = None) -> Dict:
        """Get database statistics"""
        with sqlite3.connect(self.db_path) as conn:
            if profile_id:
                # Get the profile's channel to count nodes and messages for that channel
                profile_cursor = conn.execute("""
                    SELECT channel, key FROM profiles WHERE id = ?
                """, (profile_id,))
                profile_row = profile_cursor.fetchone()
                
                if profile_row:
                    from encryption import generate_hash
                    channel = generate_hash(profile_row[0], profile_row[1])
                    
                    cursor = conn.execute("""
                        SELECT 
                            (SELECT COUNT(*) FROM nodes WHERE channel = ?) as node_count,
                            (SELECT COUNT(*) FROM messages WHERE channel = ?) as message_count
                    """, (channel, channel))
                else:
                    cursor = conn.execute("""
                        SELECT 0 as node_count, 0 as message_count
                    """)
            else:
                cursor = conn.execute("""
                    SELECT 
                        (SELECT COUNT(*) FROM profiles) as profile_count,
                        (SELECT COUNT(*) FROM nodes) as total_nodes,
                        (SELECT COUNT(*) FROM messages) as total_messages
                """)
            
            row = cursor.fetchone()
            if profile_id:
                return {
                    'nodes': row[0],
                    'messages': row[1]
                }
            else:
                return {
                    'profiles': row[0],
                    'total_nodes': row[1], 
                    'total_messages': row[2]
                }

    def get_messages_for_channel(self, channel: int, limit: int = 100) -> List[Dict]:
        """Get messages for a specific channel (accessible to any profile using that channel)"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            
            # Get messages for the specific channel
            cursor = conn.execute("""
                SELECT m.message_id, m.packet_id, m.sender_num, m.sender_display, n.short_name AS sender_short_name,
                       m.content, m.timestamp, m.sender_ip, m.direction, m.channel, m.message_type,
                       m.reply_packet_id, m.target_node_num, m.owner_profile_id, m.hop_limit, m.hop_start,
                       m.rx_snr, m.rx_rssi, m.ack_requested, m.ack_status, m.ack_error, m.ack_updated_at
                FROM messages m
                LEFT JOIN nodes n ON n.channel = m.channel AND n.node_num = m.sender_num
                WHERE m.channel = ? AND COALESCE(m.message_type, 'channel') = 'channel'
                ORDER BY m.timestamp DESC LIMIT ?
            """, (channel, limit))
            
            return self._serialize_messages(cursor)

    def get_dm_messages_for_profile(self, profile_id: str, limit: int = 200) -> List[Dict]:
        """Get direct messages scoped to a specific profile identity."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT m.message_id, m.packet_id, m.sender_num, m.sender_display, n.short_name AS sender_short_name,
                       m.content, m.timestamp, m.sender_ip, m.direction, m.channel, m.message_type,
                       m.reply_packet_id, m.target_node_num, m.owner_profile_id, m.hop_limit, m.hop_start,
                       m.rx_snr, m.rx_rssi, m.ack_requested, m.ack_status, m.ack_error, m.ack_updated_at
                FROM messages m
                LEFT JOIN nodes n ON n.channel = m.channel AND n.node_num = m.sender_num
                WHERE m.owner_profile_id = ? AND COALESCE(m.message_type, 'channel') = 'dm'
                ORDER BY m.timestamp DESC LIMIT ?
            """, (profile_id, limit))
            return self._serialize_messages(cursor)

    def update_message_ack_status(self, packet_id: int, ack_status: str, ack_error: str = None) -> List[Dict]:
        """Update ACK status for outbound messages matching a packet ID and return updated rows."""
        if packet_id is None:
            return []

        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                conn.execute(
                    """
                    UPDATE messages
                    SET ack_status = ?, ack_error = ?, ack_updated_at = CURRENT_TIMESTAMP
                    WHERE packet_id = ?
                      AND direction = 'sent'
                      AND COALESCE(ack_requested, 0) = 1
                    """,
                    (ack_status, ack_error, int(packet_id)),
                )
                cursor = conn.execute(
                    """
                    SELECT m.message_id, m.packet_id, m.sender_num, m.sender_display, n.short_name AS sender_short_name,
                           m.content, m.timestamp, m.sender_ip, m.direction, m.channel, m.message_type,
                           m.reply_packet_id, m.target_node_num, m.owner_profile_id, m.hop_limit, m.hop_start,
                           m.rx_snr, m.rx_rssi, m.ack_requested, m.ack_status, m.ack_error, m.ack_updated_at
                    FROM messages m
                    LEFT JOIN nodes n ON n.channel = m.channel AND n.node_num = m.sender_num
                    WHERE m.packet_id = ?
                      AND m.direction = 'sent'
                      AND COALESCE(m.ack_requested, 0) = 1
                    ORDER BY m.id DESC
                    """,
                    (int(packet_id),),
                )
                rows = self._serialize_messages(cursor)
                conn.commit()
                return rows
        except Exception as e:
            print(f"Error updating ACK status: {e}")
            return []

    def delete_channel_messages(self, channel: int) -> int:
        """Delete all channel messages for a specific channel hash."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute(
                    """
                    DELETE FROM messages
                    WHERE channel = ? AND COALESCE(message_type, 'channel') = 'channel'
                    """,
                    (channel,),
                )
                conn.commit()
                return int(cursor.rowcount or 0)
        except Exception as e:
            print(f"Error deleting channel messages: {e}")
            return 0

    def delete_dm_thread_for_profile(self, profile_id: str, peer_node_num: int) -> int:
        """Delete all DM messages in a thread for a given profile and peer node."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute(
                    """
                    DELETE FROM messages
                    WHERE owner_profile_id = ?
                      AND COALESCE(message_type, 'channel') = 'dm'
                      AND (sender_num = ? OR target_node_num = ?)
                    """,
                    (profile_id, int(peer_node_num), int(peer_node_num)),
                )
                conn.commit()
                return int(cursor.rowcount or 0)
        except Exception as e:
            print(f"Error deleting DM thread: {e}")
            return 0

    def get_nodes_for_profile_channel(self, profile_id: str, channel: int) -> List[Dict]:
        """Get nodes for a specific profile that have been seen on a specific channel"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            
            # Get nodes that have sent messages on the specified channel
            cursor = conn.execute("""
                SELECT DISTINCT n.node_id, n.node_num, n.long_name, n.short_name, 
                       n.macaddr, n.hw_model, n.role, n.first_seen, n.last_seen, 
                       n.packet_count, n.raw_nodeinfo
                FROM nodes n
                WHERE n.profile_id = ? 
                AND EXISTS (
                    SELECT 1 FROM messages m 
                    WHERE m.sender_num = n.node_num AND m.channel = ?
                )
                ORDER BY n.last_seen DESC
            """, (profile_id, channel))
            
            nodes = []
            for row in cursor:
                # Ensure node_id is properly formatted as hex
                node_id = row['node_id']
                if not node_id and row['node_num']:
                    # Fallback: create hex node_id from node_num
                    node_id = f"!{hex(row['node_num'])[2:].zfill(8)}"
                elif node_id and not node_id.startswith('!'):
                    # Fix malformed node_id that might be decimal
                    try:
                        # If it's a decimal number, convert to hex format
                        decimal_val = int(node_id)
                        node_id = f"!{hex(decimal_val)[2:].zfill(8)}"
                    except ValueError:
                        # If it's not a number, keep as is
                        pass
                
                node_data = {
                    'node_id': node_id,
                    'node_num': row['node_num'],
                    'long_name': row['long_name'],
                    'short_name': row['short_name'],
                    'hw_model': row['hw_model'],
                    'role': row['role'],
                    'first_seen': row['first_seen'],
                    'last_seen': row['last_seen'],
                    'packet_count': row['packet_count']
                }
                
                # Try to parse raw_nodeinfo JSON
                if row['raw_nodeinfo']:
                    try:
                        node_data['raw_nodeinfo'] = json.loads(row['raw_nodeinfo'])
                    except:
                        pass
                        
                # Format MAC address if available
                if row['macaddr']:
                    try:
                        mac_bytes = row['macaddr']
                        node_data['macaddr'] = ':'.join(f'{b:02x}' for b in mac_bytes)
                    except:
                        pass
                        
                nodes.append(node_data)
            return nodes
    
    def update_profile_last_seen(self, profile_id: str, channel: int) -> bool:
        """Update the last seen timestamp for a profile+channel combination"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Update or insert last seen record
                conn.execute("""
                    INSERT OR REPLACE INTO profile_last_seen 
                    (profile_id, channel, last_seen_timestamp)
                    VALUES (?, ?, CURRENT_TIMESTAMP)
                """, (profile_id, channel))
                conn.commit()
                return True
        except Exception as e:
            print(f"Error updating profile last seen: {e}")
            return False
    
    def get_unread_messages_for_channel(self, profile_id: str, channel: int, limit: int = 100) -> List[Dict]:
        """Get unread messages for a specific channel (based on profile's last seen timestamp)"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            
            # Get the last seen timestamp for this profile+channel combination
            cursor = conn.execute("""
                SELECT last_seen_timestamp FROM profile_last_seen 
                WHERE profile_id = ? AND channel = ?
            """, (profile_id, channel))
            last_seen_row = cursor.fetchone()
            
            if last_seen_row and last_seen_row['last_seen_timestamp']:
                # Get messages newer than last seen
                cursor = conn.execute("""
                    SELECT m.message_id, m.packet_id, m.sender_num, m.sender_display, n.short_name AS sender_short_name,
                           m.content, m.timestamp, m.sender_ip, m.direction, m.channel, m.message_type,
                           m.reply_packet_id, m.target_node_num, m.owner_profile_id, m.hop_limit, m.hop_start,
                           m.rx_snr, m.rx_rssi, m.ack_requested, m.ack_status, m.ack_error, m.ack_updated_at
                    FROM messages m
                    LEFT JOIN nodes n ON n.channel = m.channel AND n.node_num = m.sender_num
                    WHERE m.channel = ? AND COALESCE(m.message_type, 'channel') = 'channel' AND m.timestamp > ?
                    ORDER BY m.timestamp DESC LIMIT ?
                """, (channel, last_seen_row['last_seen_timestamp'], limit))
            else:
                # No last seen timestamp - return all messages (but limit to prevent overload)
                cursor = conn.execute("""
                    SELECT m.message_id, m.packet_id, m.sender_num, m.sender_display, n.short_name AS sender_short_name,
                           m.content, m.timestamp, m.sender_ip, m.direction, m.channel, m.message_type,
                           m.reply_packet_id, m.target_node_num, m.owner_profile_id, m.hop_limit, m.hop_start,
                           m.rx_snr, m.rx_rssi, m.ack_requested, m.ack_status, m.ack_error, m.ack_updated_at
                    FROM messages m
                    LEFT JOIN nodes n ON n.channel = m.channel AND n.node_num = m.sender_num
                    WHERE m.channel = ? AND COALESCE(m.message_type, 'channel') = 'channel'
                    ORDER BY m.timestamp DESC LIMIT ?
                """, (channel, limit))

            return self._serialize_messages(cursor)

    def _serialize_messages(self, cursor) -> List[Dict]:
        messages = []
        for row in cursor:
            sender_num = row['sender_num']
            target_node_num = row['target_node_num']

            if row['sender_display']:
                current_sender_display = row['sender_display']
            elif sender_num:
                current_sender_display = f"!{hex(sender_num)[2:].zfill(8)}"
            else:
                current_sender_display = 'Unknown'

            messages.append({
                'id': row['message_id'],
                'packet_id': row['packet_id'],
                'reply_packet_id': row['reply_packet_id'],
                'sender_num': sender_num,
                'sender': f"!{hex(sender_num)[2:].zfill(8)}" if sender_num else 'Unknown',
                'sender_display': current_sender_display,
                'sender_short_name': row['sender_short_name'],
                'content': row['content'],
                'timestamp': row['timestamp'],
                'sender_ip': row['sender_ip'],
                'direction': row['direction'],
                'channel': row['channel'],
                'message_type': row['message_type'] or 'channel',
                'target_node_num': target_node_num,
                'target': f"!{hex(target_node_num)[2:].zfill(8)}" if target_node_num else None,
                'owner_profile_id': row['owner_profile_id'],
                'hop_limit': row['hop_limit'],
                'hop_start': row['hop_start'],
                'rx_snr': row['rx_snr'],
                'rx_rssi': row['rx_rssi'],
                'ack_requested': bool(row['ack_requested']) if row['ack_requested'] is not None else False,
                'ack_status': row['ack_status'],
                'ack_error': row['ack_error'],
                'ack_updated_at': row['ack_updated_at'],
            })
        return messages[::-1]
